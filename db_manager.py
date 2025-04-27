# fhir_processor/db_manager.py
import sqlite3
import logging
# Import schema and format helper from config
from config import TABLE_DEFINITIONS, format_datetime_for_db
import os

# Configure logging if not already done centrally
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def safe_get_for_db(data, key, default=None):
    """Get value from dict, return default if None or key missing."""
    if not isinstance(data, dict):
        logging.warning(f"Attempted safe_get on non-dict: {type(data)}")
        return default
    val = data.get(key, default)
    return val

def create_connection(db_file):
    """Create a database connection to the SQLite database specified by db_file."""
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        conn.execute("PRAGMA foreign_keys = ON;")
        logging.info(f"SQLite DB connection successful to {db_file} (version {sqlite3.sqlite_version}), Foreign Keys ON.")
    except sqlite3.Error as e:
        logging.error(f"Error connecting to database {db_file}: {e}")
        if conn:
            conn.close()
        conn = None
    return conn

def create_tables(conn):
    """Create tables from the TABLE_DEFINITIONS dictionary if they don't exist."""
    if not conn:
        logging.error("Database connection is not valid. Cannot create tables.")
        return False
    try:
        cursor = conn.cursor()
        cursor.execute("PRAGMA foreign_keys = ON;")
        logging.info("Ensuring database tables exist...")
        for table_name, definition in TABLE_DEFINITIONS.items():
            logging.debug(f"Executing schema for table {table_name}")
            cursor.execute(definition)
        conn.commit()
        logging.info("Tables checked/created successfully.")
        return True
    except sqlite3.Error as e:
        logging.error(f"Error creating tables: {e}")
        try:
            conn.rollback()
        except sqlite3.Error as rb_err:
            logging.error(f"Error during rollback after table creation failure: {rb_err}")
        return False

def insert_records(conn, table_name, data_list, columns):
    """
    Generic function to insert multiple records into a table using INSERT OR IGNORE.
    Uses executemany for efficiency. Returns number of rows affected by the last statement.
    """
    if not conn:
        logging.error(f"Database connection is not valid. Cannot insert records into {table_name}.")
        return -1
    if not data_list:
        logging.info(f"No data provided for table {table_name}, skipping insertion.")
        return 0
    if not columns:
        logging.error(f"No columns specified for table {table_name}. Cannot insert.")
        return -1
    placeholders = ', '.join('?' * len(columns))
    sql = f''' INSERT OR IGNORE INTO {table_name}({', '.join(columns)})
               VALUES({placeholders}) '''

    data_tuples = []
    for record in data_list:
        if isinstance(record, dict):
            try:
                tuple_data = tuple(record.get(col) for col in columns)
                data_tuples.append(tuple_data)
            except Exception as e:
                 logging.warning(f"Error creating tuple for record in {table_name}: {record} - {e}")
        else:
            logging.warning(f"Skipping non-dictionary record during insertion preparation for {table_name}: {record}")

    if not data_tuples:
        logging.warning(f"No valid data tuples generated for table {table_name}.")
        return 0

    inserted_count = 0
    logging.debug(f"Attempting to insert {len(data_tuples)} records into {table_name} using executemany...")
    if data_tuples:
         logging.debug(f"First data tuple sample for {table_name}: {data_tuples[0]}")

    try:
        cursor = conn.cursor()
        cursor.execute("PRAGMA foreign_keys = ON;")
        cursor.executemany(sql, data_tuples)
        inserted_count = cursor.rowcount
        conn.commit()
        logging.info(f"Successfully executed INSERT OR IGNORE for {len(data_tuples)} records into {table_name}. Rows affected (likely new rows): {inserted_count}.")
    except sqlite3.IntegrityError as ie:
        logging.error(f"SQLite IntegrityError during bulk insert into {table_name}: {ie}")
        logging.error(f"  SQL attempted: {sql}")
        if data_tuples: logging.error(f"  First data tuple sample: {data_tuples[0]}")
        conn.rollback()
        inserted_count = -1
    except sqlite3.Error as e:
        logging.error(f"Database error during bulk insert into {table_name}: {e}")
        logging.error(f"  SQL attempted: {sql}")
        if data_tuples: logging.error(f"  First data tuple sample: {data_tuples[0]}")
        conn.rollback()
        inserted_count = -1
    except Exception as e:
         logging.error(f"Unexpected Python error during bulk insert into {table_name}: {e}", exc_info=True)
         conn.rollback()
         inserted_count = -1

    return inserted_count

def insert_records_debug_mode(conn, table_name, data_list, columns):
    """
    Inserts records one by one for debugging FOREIGN KEY or other integrity errors.
    Logs the specific record causing the failure. Slower than executemany.
    Returns tuple: (success_count, failure_count)
    """
    if not conn:
        logging.error("[Debug Mode] Database connection is not valid. Cannot insert records.")
        return -1, 0
    if not data_list:
        logging.info(f"[Debug Mode] No data provided for table {table_name}, skipping insertion.")
        return 0, 0
    if not columns:
        logging.error(f"[Debug Mode] No columns specified for table {table_name}. Cannot insert.")
        return -1, 0
    placeholders = ', '.join('?' * len(columns))
    sql = f''' INSERT OR IGNORE INTO {table_name}({', '.join(columns)})
               VALUES({placeholders}) '''

    success_count = 0
    failure_count = 0
    cursor = conn.cursor()
    cursor.execute("PRAGMA foreign_keys = ON;")
    logging.info(f"[Debug Mode] Inserting {len(data_list)} records into {table_name} one by one...")

    for i, record in enumerate(data_list):
        record_tuple = None
        try:
            if isinstance(record, dict):
                record_tuple = tuple(record.get(col) for col in columns)
            else:
                logging.warning(f"[Debug Mode] Skipping non-dictionary record at index {i} for {table_name}: {record}")
                failure_count += 1
                continue

            cursor.execute(sql, record_tuple)
            conn.commit()
            success_count += 1

        except sqlite3.IntegrityError as ie:
            logging.error(f"[Debug Mode] IntegrityError inserting record #{i+1} into {table_name}: {ie}")
            logging.error(f"  Offending SQL: {sql}")
            logging.error(f"  Offending Data Dict: {record}")
            logging.error(f"  Offending Data Tuple: {record_tuple}")
            # --- Add detailed FK checks ---
            if table_name == 'encounters':
                 check_encounter_fks(conn, record)
            elif table_name == 'practitioner_roles':
                 check_role_fks(conn, record)
            elif table_name == 'schedules': # New
                 check_schedule_fks(conn, record)
            elif table_name == 'slots': # New
                 check_slot_fks(conn, record)
            # --- End FK checks ---
            conn.rollback()
            failure_count += 1
        except sqlite3.Error as e:
            logging.error(f"[Debug Mode] Database error inserting record #{i+1} into {table_name}: {e}")
            logging.error(f"  Offending Data Dict: {record}")
            logging.error(f"  Offending Data Tuple: {record_tuple}")
            conn.rollback()
            failure_count += 1
        except Exception as e:
            logging.error(f"[Debug Mode] Unexpected Python error processing record #{i+1} for {table_name}: {e}", exc_info=True)
            logging.error(f"  Offending Data Dict: {record}")
            try:
                conn.rollback()
            except sqlite3.Error as rb_err:
                 logging.error(f"[Debug Mode] Error during rollback after unexpected error: {rb_err}")
            failure_count += 1

    logging.info(f"[Debug Mode] Finished inserting into {table_name}. Succeeded/Ignored: {success_count}, Failed: {failure_count}.")
    return success_count, failure_count

# --- Helper functions to check Foreign Keys ---
def check_encounter_fks(conn, encounter_record):
    """Checks if FKs for a given encounter record exist in parent tables."""
    cursor = conn.cursor()
    patient_id = encounter_record.get('patient_fhir_id')
    practitioner_npi = encounter_record.get('practitioner_npi')
    hospital_id = encounter_record.get('hospital_fhir_id')
    encounter_id = encounter_record.get('encounter_id', 'N/A')
    logging.debug(f"  Performing FK check for Encounter ID: {encounter_id}")
    if patient_id:
        cursor.execute("SELECT 1 FROM patients WHERE patient_fhir_id = ?", (patient_id,))
        if not cursor.fetchone(): logging.error(f"  FK Check Failed: Patient '{patient_id}' not found.")
        else: logging.debug(f"  FK Check OK: Patient '{patient_id}' found.")
    else: logging.error(f"  FK Check Warning: Encounter record has NULL patient_fhir_id.")
    if practitioner_npi:
        cursor.execute("SELECT 1 FROM practitioners WHERE practitioner_npi = ?", (practitioner_npi,))
        if not cursor.fetchone(): logging.error(f"  FK Check Failed: Practitioner '{practitioner_npi}' not found.")
        else: logging.debug(f"  FK Check OK: Practitioner '{practitioner_npi}' found.")
    else: logging.debug(f"  FK Check Info: Encounter has NULL practitioner_npi (allowed).")
    if hospital_id:
        cursor.execute("SELECT 1 FROM hospitals WHERE hospital_fhir_id = ?", (hospital_id,))
        if not cursor.fetchone(): logging.error(f"  FK Check Failed: Hospital '{hospital_id}' not found.")
        else: logging.debug(f"  FK Check OK: Hospital '{hospital_id}' found.")
    else: logging.debug(f"  FK Check Info: Encounter has NULL hospital_fhir_id (allowed).")

def check_role_fks(conn, role_record):
    """Checks if FKs for a given role record exist in parent tables."""
    cursor = conn.cursor()
    practitioner_npi = role_record.get('practitioner_npi')
    hospital_id = role_record.get('hospital_fhir_id')
    role_display = role_record.get('role_display', 'N/A')
    logging.debug(f"  Performing FK check for Role: {role_display} (Practitioner: {practitioner_npi})")
    if practitioner_npi:
        cursor.execute("SELECT 1 FROM practitioners WHERE practitioner_npi = ?", (practitioner_npi,))
        if not cursor.fetchone(): logging.error(f"  FK Check Failed: Practitioner '{practitioner_npi}' not found (for role).")
        else: logging.debug(f"  FK Check OK: Practitioner '{practitioner_npi}' found.")
    else: logging.error(f"  FK Check Warning: Practitioner Role record has NULL practitioner_npi.")
    if hospital_id:
        cursor.execute("SELECT 1 FROM hospitals WHERE hospital_fhir_id = ?", (hospital_id,))
        if not cursor.fetchone(): logging.error(f"  FK Check Failed: Hospital '{hospital_id}' not found (for role).")
        else: logging.debug(f"  FK Check OK: Hospital '{hospital_id}' found.")
    else: logging.debug(f"  FK Check Info: Role has NULL hospital_fhir_id (allowed).")

# --- NEW FK Checkers ---
def check_schedule_fks(conn, schedule_record):
    """Checks if FKs for a given schedule record exist in parent tables."""
    cursor = conn.cursor()
    practitioner_npi = schedule_record.get('practitioner_npi')
    schedule_id = schedule_record.get('schedule_fhir_id', 'N/A')
    logging.debug(f"  Performing FK check for Schedule ID: {schedule_id}")
    if practitioner_npi:
        cursor.execute("SELECT 1 FROM practitioners WHERE practitioner_npi = ?", (practitioner_npi,))
        if not cursor.fetchone(): logging.error(f"  FK Check Failed: Practitioner '{practitioner_npi}' not found (for schedule).")
        else: logging.debug(f"  FK Check OK: Practitioner '{practitioner_npi}' found.")
    else: logging.error(f"  FK Check Warning: Schedule record has NULL practitioner_npi.")

def check_slot_fks(conn, slot_record):
    """Checks if FKs for a given slot record exist in parent tables."""
    cursor = conn.cursor()
    schedule_id = slot_record.get('schedule_fhir_id')
    slot_id = slot_record.get('slot_fhir_id', 'N/A')
    logging.debug(f"  Performing FK check for Slot ID: {slot_id}")
    if schedule_id:
        cursor.execute("SELECT 1 FROM schedules WHERE schedule_fhir_id = ?", (schedule_id,))
        if not cursor.fetchone(): logging.error(f"  FK Check Failed: Schedule '{schedule_id}' not found (for slot).")
        else: logging.debug(f"  FK Check OK: Schedule '{schedule_id}' found.")
    else: logging.error(f"  FK Check Warning: Slot record has NULL schedule_fhir_id.")
# --- End NEW FK Checkers ---


# --- Specific Insertion Functions (call the generic one or debug one) ---
def insert_patients(conn, patients, debug=False):
    """Inserts patient data."""
    columns = [
    'patient_fhir_id', 'first_name', 'middle_name', 'last_name', 'prefix',
    'mothers_maiden_name', 'dob', 'gender', 'marital_status', 'ssn',
    'drivers_license', 'passport', 'mrn', 'mrn_system', 'phone_home', 'address_line',
    'address_city', 'address_state', 'address_postal_code', 'address_country',
    'birth_city', 'birth_state', 'birth_country', 'language'
    ]
    if debug:
        logging.warning("Running patient insert in debug mode (row-by-row).")
        return insert_records_debug_mode(conn, 'patients', patients, columns)
    else:
        return insert_records(conn, 'patients', patients, columns)

def insert_hospitals(conn, hospitals, debug=False):
    """Inserts hospital data."""
    columns = [
    'hospital_fhir_id', 'synthea_identifier', 'name', 'phone', 'address_line',
    'address_city', 'address_state', 'address_postal_code', 'address_country'
    ]
    if debug:
        logging.warning("Running hospital insert in debug mode (row-by-row).")
        return insert_records_debug_mode(conn, 'hospitals', hospitals, columns)
    else:
        return insert_records(conn, 'hospitals', hospitals, columns)

def insert_practitioners(conn, practitioners, debug=False):
    """Inserts practitioner data."""
    columns = [
    'practitioner_npi', 'first_name', 'last_name', 'prefix', 'email', 'gender',
    'address_line', 'address_city', 'address_state', 'address_postal_code', 'address_country'
    ]
    if debug:
        logging.warning("Running practitioner insert in debug mode (row-by-row).")
        return insert_records_debug_mode(conn, 'practitioners', practitioners, columns)
    else:
        return insert_records(conn, 'practitioners', practitioners, columns)

def insert_practitioner_roles(conn, roles, debug=False):
    """Inserts practitioner role data."""
    columns = [
    'practitioner_npi', 'hospital_fhir_id', 'role_code', 'role_system', 'role_display',
    'specialty_code', 'specialty_system', 'specialty_display'
    ]
    if debug:
        return insert_records_debug_mode(conn, 'practitioner_roles', roles, columns)
    else:
        return insert_records(conn, 'practitioner_roles', roles, columns)

def insert_encounters(conn, encounters, debug=False):
    """Inserts encounter data."""
    columns = [
    'encounter_id', 'patient_fhir_id', 'practitioner_npi', 'hospital_fhir_id',
    'start_time', 'end_time', 'encounter_class_code', 'encounter_type_code',
    'encounter_type_system', 'encounter_type_display'
    ]
    if debug:
        return insert_records_debug_mode(conn, 'encounters', encounters, columns)
    else:
        return insert_records(conn, 'encounters', encounters, columns)

# --- NEW Insertion Functions ---
def insert_schedules(conn, schedules, debug=False):
    """Inserts schedule data."""
    columns = [
        'schedule_fhir_id', 'practitioner_npi', 'active',
        'planning_horizon_start', 'planning_horizon_end', 'comment'
    ]
    if debug:
        return insert_records_debug_mode(conn, 'schedules', schedules, columns)
    else:
        return insert_records(conn, 'schedules', schedules, columns)

def insert_slots(conn, slots, debug=False):
    """Inserts slot data."""
    columns = [
        'slot_fhir_id', 'schedule_fhir_id', 'status',
        'start_time', 'end_time', 'comment'
    ]
    if debug:
        return insert_records_debug_mode(conn, 'slots', slots, columns)
    else:
        return insert_records(conn, 'slots', slots, columns)
# --- End NEW Insertion Functions ---

# --- Main block for basic standalone testing ---
if __name__ == "__main__":
    # Use DEBUG level for standalone testing to see more details
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info("Running db_manager.py script directly for basic tests...")

    current_dir = os.path.dirname(os.path.abspath(__file__))
    test_db_name = os.path.join(current_dir, "test_synthea_db_manager.db")

    # Clean up old test DB file if it exists
    if test_db_name != ":memory:" and os.path.exists(test_db_name):
        try:
            os.remove(test_db_name)
            logging.info(f"Removed existing test database file: {test_db_name}")
        except OSError as e:
             logging.error(f"Error removing existing test database {test_db_name}: {e}")

    conn = create_connection(test_db_name)
    if conn:
        print("-" * 30)
        print("Connection Test: SUCCESS")
        if create_tables(conn):
             print("Create Tables Test: SUCCESS")
             print("Testing dummy data insertion...")

             # Dummy Hospital (Parent)
             dummy_hospital = [{'hospital_fhir_id': 'test-hosp-1', 'name': 'Test Hospital'}]
             affected_hosp = insert_hospitals(conn, dummy_hospital)
             print(f" Dummy Hospital Insert Result (affected rows): {affected_hosp}")

             # Dummy Practitioner (Parent)
             dummy_practitioner = [{'practitioner_npi': 'NPI-TEST-123', 'first_name': 'Test', 'last_name': 'Doctor'}]
             affected_prac = insert_practitioners(conn, dummy_practitioner)
             print(f" Dummy Practitioner Insert Result (affected rows): {affected_prac}")

             # Dummy Patient (Parent)
             dummy_patient = [{'patient_fhir_id': 'test-pat-1', 'first_name': 'Dummy', 'last_name': 'Patient'}]
             affected_pat = insert_patients(conn, dummy_patient)
             print(f" Dummy Patient Insert Result (affected rows): {affected_pat}")

             # --- NEW: Test Schedule Insertion ---
             # Dummy Schedule (Child of Practitioner - should succeed)
             dummy_schedule_good = [{
                 'schedule_fhir_id': 'sch-good-1', 'practitioner_npi': 'NPI-TEST-123',
                 'planning_horizon_start': '2024-01-01T00:00:00Z',
                 'planning_horizon_end': '2024-01-31T23:59:59Z',
                 'active': 1, 'comment': 'Test Schedule'
             }]
             print(" Testing valid schedule insertion (debug mode)...")
             s_sch_good, f_sch_good = insert_schedules(conn, dummy_schedule_good, debug=True)
             print(f" Dummy Valid Schedule Insert Result (Success: {s_sch_good}, Failed: {f_sch_good})")
             if s_sch_good == 1 and f_sch_good == 0: print("  Valid Schedule Insert Test: SUCCESS")
             else: print("  Valid Schedule Insert Test: FAILED")

             # Dummy Schedule (Child of Practitioner - should fail FK)
             # dummy_schedule_bad_fk = [{
             #     'schedule_fhir_id': 'sch-bad-fk-1', 'practitioner_npi': 'NPI-NONEXISTENT', # Bad NPI
             #     'planning_horizon_start': '2024-02-01T00:00:00Z',
             #     'planning_horizon_end': '2024-02-28T23:59:59Z',
             #     'active': 1
             # }]
             # print(" Testing invalid schedule insertion (debug mode)...")
             # s_sch_bad, f_sch_bad = insert_schedules(conn, dummy_schedule_bad_fk, debug=True)
             # print(f" Dummy Invalid Schedule Insert Result (Success: {s_sch_bad}, Failed: {f_sch_bad})")
             # if s_sch_bad == 0 and f_sch_bad == 1: print("  Invalid Schedule Insert Test (FK Fail): SUCCESS")
             # else: print("  Invalid Schedule Insert Test (FK Fail): FAILED")
             # # --- End NEW Schedule Test ---

             # --- NEW: Test Slot Insertion ---
             # Dummy Slot (Child of Schedule - should succeed)
             dummy_slot_good = [{
                 'slot_fhir_id': 'slot-good-1', 'schedule_fhir_id': 'sch-good-1', # Valid Schedule ID
                 'status': 'free', 'start_time': '2024-01-01T09:00:00Z', 'end_time': '2024-01-01T09:30:00Z'
             }]
             print(" Testing valid slot insertion (debug mode)...")
             s_slot_good, f_slot_good = insert_slots(conn, dummy_slot_good, debug=True)
             print(f" Dummy Valid Slot Insert Result (Success: {s_slot_good}, Failed: {f_slot_good})")
             if s_slot_good == 1 and f_slot_good == 0: print("  Valid Slot Insert Test: SUCCESS")
             else: print("  Valid Slot Insert Test: FAILED")

             # # Dummy Slot (Child of Schedule - should fail FK)
             # dummy_slot_bad_fk = [{
             #     'slot_fhir_id': 'slot-bad-fk-1', 'schedule_fhir_id': 'sch-NONEXISTENT', # Bad Schedule ID
             #     'status': 'busy', 'start_time': '2024-01-01T10:00:00Z', 'end_time': '2024-01-01T10:15:00Z'
             # }]
             # print(" Testing invalid slot insertion (debug mode)...")
             # s_slot_bad, f_slot_bad = insert_slots(conn, dummy_slot_bad_fk, debug=True)
             # print(f" Dummy Invalid Slot Insert Result (Success: {s_slot_bad}, Failed: {f_slot_bad})")
             # if s_slot_bad == 0 and f_slot_bad == 1: print("  Invalid Slot Insert Test (FK Fail): SUCCESS")
             # else: print("  Invalid Slot Insert Test (FK Fail): FAILED")
             # # --- End NEW Slot Test ---


             # Dummy Encounter (Child - should succeed)
             dummy_encounter_good = [{
                 'encounter_id': 'enc-good-1', 'patient_fhir_id': 'test-pat-1',
                 'practitioner_npi': 'NPI-TEST-123', 'hospital_fhir_id': 'test-hosp-1',
                 'start_time': '2023-01-01T10:00:00Z'
             }]
             print(" Testing valid encounter insertion (debug mode)...")
             s_good, f_good = insert_encounters(conn, dummy_encounter_good, debug=True)
             print(f" Dummy Valid Encounter Insert Result (Success: {s_good}, Failed: {f_good})")
             if s_good == 1 and f_good == 0: print("  Valid Encounter Insert Test: SUCCESS")
             else: print("  Valid Encounter Insert Test: FAILED")

             # # Dummy Encounter (Child - should fail FK constraint)
             # dummy_encounter_bad_fk = [{
             #     'encounter_id': 'enc-bad-fk-1', 'patient_fhir_id': 'test-pat-NONEXISTENT', # Bad Patient ID
             #     'practitioner_npi': 'NPI-TEST-123', 'hospital_fhir_id': 'test-hosp-1',
             #     'start_time': '2023-01-01T11:00:00Z'
             # }]
             # print(" Testing invalid encounter insertion (debug mode)...")
             # s_bad, f_bad = insert_encounters(conn, dummy_encounter_bad_fk, debug=True)
             # print(f" Dummy Invalid Encounter Insert Result (Success: {s_bad}, Failed: {f_bad})")
             # if s_bad == 0 and f_bad == 1: print("  Invalid Encounter Insert Test (FK Fail): SUCCESS")
             # else: print("  Invalid Encounter Insert Test (FK Fail): FAILED")

             # Test ignoring a duplicate patient (non-debug mode)
             print(" Testing duplicate patient insertion (should be ignored)...")
             affected_again = insert_patients(conn, dummy_patient)
             print(f" Duplicate Patient Insert Result (affected rows): {affected_again}")
             if affected_again == 0: print("  Duplicate Patient Ignore Test: SUCCESS")
             else: print(f"  Duplicate Patient Ignore Test: FAILED (affected={affected_again})")

        else:
             print("Create Tables Test: FAILED")
        conn.close()
        print("Connection Closed.")
        # Optionally keep the test db for inspection
        # if test_db_name != ":memory:" and os.path.exists(test_db_name):
        #     # os.remove(test_db_name) # Keep it for inspection
        #     logging.info(f"Test database kept: {test_db_name}")
    else:
        print("-" * 30)
        print("Connection Test: FAILED")
    print("-" * 30)
