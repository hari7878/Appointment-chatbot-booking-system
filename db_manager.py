# fhir_processor/db_manager.py
import sqlite3
import logging
from config import TABLE_DEFINITIONS # Import schema from config
import os
# Configure logging if not already done centrally
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def safe_get_for_db(data, key, default=None):
    """Get value from dict, return default if None or key missing."""
    # Ensure data is a dictionary before attempting get
    if not isinstance(data, dict):
        logging.warning(f"Attempted safe_get on non-dict: {type(data)}")
        return default
    val = data.get(key, default)
    # SQLite handles Python None as NULL automatically
    return val # Simplified: just return the value or None/default

def create_connection(db_file):
    """Create a database connection to the SQLite database specified by db_file."""
    conn = None
    try:
        # Explicitly enable foreign key support before any operations
        conn = sqlite3.connect(db_file)
        conn.execute("PRAGMA foreign_keys = ON;")
        logging.info(f"SQLite DB connection successful to {db_file} (version {sqlite3.sqlite_version}), Foreign Keys ON.")
    except sqlite3.Error as e:
        logging.error(f"Error connecting to database {db_file}: {e}")
        if conn:
            conn.close() # Close connection if PRAGMA failed but connection object exists
        conn = None # Ensure conn is None on error
    return conn

def create_tables(conn):
    """Create tables from the TABLE_DEFINITIONS dictionary if they don't exist."""
    if not conn:
        logging.error("Database connection is not valid. Cannot create tables.")
        return False
    try:
        cursor = conn.cursor()
        # Ensure foreign keys are enabled for this cursor's operations as well
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
        # Attempt rollback if something went wrong during table creation
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
        return -1 # Indicate error state
    if not data_list:
        logging.info(f"No data provided for table {table_name}, skipping insertion.")
        return 0
    if not columns:
        logging.error(f"No columns specified for table {table_name}. Cannot insert.")
        return -1 # Indicate error state

    placeholders = ', '.join('?' * len(columns))
    # Use INSERT OR IGNORE to skip duplicates based on PRIMARY KEY or UNIQUE constraints
    sql = f''' INSERT OR IGNORE INTO {table_name}({', '.join(columns)})
               VALUES({placeholders}) '''

    data_tuples = []
    for record in data_list:
        if isinstance(record, dict):
            try:
                # Create tuple ensuring order matches columns list and handle potential None values
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
         # Log only a small sample to avoid excessive output
         logging.debug(f"First data tuple sample for {table_name}: {data_tuples[0]}")

    try:
        cursor = conn.cursor()
        # Ensure foreign keys are enabled for this operation
        cursor.execute("PRAGMA foreign_keys = ON;")
        cursor.executemany(sql, data_tuples)
        inserted_count = cursor.rowcount # Get rows affected *by this specific statement*
        conn.commit()
        # Log level INFO is appropriate here as it's a summary of a major step
        logging.info(f"Successfully executed INSERT OR IGNORE for {len(data_tuples)} records into {table_name}. Rows affected (likely new rows): {inserted_count}.")
    except sqlite3.IntegrityError as ie:
        # This specifically catches PK, UNIQUE, NOT NULL, CHECK, and FK constraint violations
        logging.error(f"SQLite IntegrityError during bulk insert into {table_name}: {ie}")
        logging.error(f"  SQL attempted: {sql}")
        if data_tuples: logging.error(f"  First data tuple sample: {data_tuples[0]}")
        conn.rollback() # Rollback the entire transaction
        inserted_count = -1 # Indicate error
    except sqlite3.Error as e:
        # Catches other SQLite errors (e.g., operational errors)
        logging.error(f"Database error during bulk insert into {table_name}: {e}")
        logging.error(f"  SQL attempted: {sql}")
        if data_tuples: logging.error(f"  First data tuple sample: {data_tuples[0]}")
        conn.rollback()
        inserted_count = -1
    except Exception as e:
         # Catch any other unexpected Python errors
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
        return -1, 0 # Indicate error, 0 failures counted yet
    if not data_list:
        logging.info(f"[Debug Mode] No data provided for table {table_name}, skipping insertion.")
        return 0, 0
    if not columns:
        logging.error(f"[Debug Mode] No columns specified for table {table_name}. Cannot insert.")
        return -1, 0 # Indicate error

    placeholders = ', '.join('?' * len(columns))
    # Still use INSERT OR IGNORE, as we might hit duplicates even in debug mode,
    # but we want to focus on other IntegrityErrors like FK violations.
    sql = f''' INSERT OR IGNORE INTO {table_name}({', '.join(columns)})
               VALUES({placeholders}) '''

    success_count = 0
    failure_count = 0
    cursor = conn.cursor()
    # Ensure foreign keys are ON for the debug cursor too
    cursor.execute("PRAGMA foreign_keys = ON;")

    logging.info(f"[Debug Mode] Inserting {len(data_list)} records into {table_name} one by one...")

    for i, record in enumerate(data_list):
        record_tuple = None
        try:
            if isinstance(record, dict):
                # Create tuple just before execution attempt
                record_tuple = tuple(record.get(col) for col in columns)
            else:
                logging.warning(f"[Debug Mode] Skipping non-dictionary record at index {i} for {table_name}: {record}")
                failure_count += 1
                continue # Skip to the next record

            cursor.execute(sql, record_tuple)
            # Commit after each successful/ignored insert in debug mode to isolate issues
            # and ensure previous successful inserts are saved before a potential error.
            conn.commit()
            # We count successful executions, which include ignored ones in this context.
            success_count += 1

        except sqlite3.IntegrityError as ie:
            # This is the key error we want to catch and diagnose in debug mode
            logging.error(f"[Debug Mode] IntegrityError inserting record #{i+1} into {table_name}: {ie}")
            logging.error(f"  Offending SQL: {sql}")
            logging.error(f"  Offending Data Dict: {record}")
            logging.error(f"  Offending Data Tuple: {record_tuple}")
            # --- Add detailed FK checks for relevant tables ---
            if table_name == 'encounters':
                 check_encounter_fks(conn, record)
            elif table_name == 'practitioner_roles':
                 check_role_fks(conn, record)
            # --- End FK checks ---
            conn.rollback() # Rollback the single failed transaction
            failure_count += 1
        except sqlite3.Error as e:
            # Catch other potential DB errors for the single record
            logging.error(f"[Debug Mode] Database error inserting record #{i+1} into {table_name}: {e}")
            logging.error(f"  Offending Data Dict: {record}")
            logging.error(f"  Offending Data Tuple: {record_tuple}")
            conn.rollback()
            failure_count += 1
        except Exception as e:
            # Catch unexpected Python errors during tuple creation or execution
            logging.error(f"[Debug Mode] Unexpected Python error processing record #{i+1} for {table_name}: {e}", exc_info=True)
            logging.error(f"  Offending Data Dict: {record}")
            # Attempt rollback just in case a transaction started
            try:
                conn.rollback()
            except sqlite3.Error as rb_err:
                 logging.error(f"[Debug Mode] Error during rollback after unexpected error: {rb_err}")
            failure_count += 1

    logging.info(f"[Debug Mode] Finished inserting into {table_name}. Succeeded/Ignored: {success_count}, Failed: {failure_count}.")
    # Return counts rather than affected rows like the bulk insert
    return success_count, failure_count

# --- Helper function to check Foreign Keys specifically for Encounters ---
def check_encounter_fks(conn, encounter_record):
    """Checks if FKs for a given encounter record exist in parent tables."""
    cursor = conn.cursor()
    patient_id = encounter_record.get('patient_fhir_id')
    practitioner_npi = encounter_record.get('practitioner_npi')
    hospital_id = encounter_record.get('hospital_fhir_id')
    encounter_id = encounter_record.get('encounter_id', 'N/A') # For logging context

    logging.debug(f"  Performing FK check for Encounter ID: {encounter_id}")

    if patient_id:
        cursor.execute("SELECT 1 FROM patients WHERE patient_fhir_id = ?", (patient_id,))
        if not cursor.fetchone():
            logging.error(f"  FK Check Failed: Patient with patient_fhir_id '{patient_id}' not found in 'patients' table.")
        else:
            logging.debug(f"  FK Check OK: Patient '{patient_id}' found.")
    else:
        # This should not happen based on schema (NOT NULL), but check anyway
        logging.error(f"  FK Check Warning: Encounter record has NULL patient_fhir_id.")


    # Practitioner NPI is nullable in encounters table
    if practitioner_npi:
        cursor.execute("SELECT 1 FROM practitioners WHERE practitioner_npi = ?", (practitioner_npi,))
        if not cursor.fetchone():
            logging.error(f"  FK Check Failed: Practitioner with practitioner_npi '{practitioner_npi}' not found in 'practitioners' table.")
        else:
             logging.debug(f"  FK Check OK: Practitioner '{practitioner_npi}' found.")
    else:
        logging.debug(f"  FK Check Info: Encounter has NULL practitioner_npi (allowed).")

    # Hospital FHIR ID is nullable in encounters table
    if hospital_id:
        cursor.execute("SELECT 1 FROM hospitals WHERE hospital_fhir_id = ?", (hospital_id,))
        if not cursor.fetchone():
            logging.error(f"  FK Check Failed: Hospital with hospital_fhir_id '{hospital_id}' not found in 'hospitals' table.")
        else:
             logging.debug(f"  FK Check OK: Hospital '{hospital_id}' found.")
    else:
        logging.debug(f"  FK Check Info: Encounter has NULL hospital_fhir_id (allowed).")

# --- Helper function to check Foreign Keys specifically for Roles ---
def check_role_fks(conn, role_record):
     """Checks if FKs for a given role record exist in parent tables."""
     cursor = conn.cursor()
     practitioner_npi = role_record.get('practitioner_npi')
     hospital_id = role_record.get('hospital_fhir_id') # Can be NULL
     role_display = role_record.get('role_display', 'N/A') # For logging context

     logging.debug(f"  Performing FK check for Role: {role_display} (Practitioner: {practitioner_npi})")

     if practitioner_npi:
        cursor.execute("SELECT 1 FROM practitioners WHERE practitioner_npi = ?", (practitioner_npi,))
        if not cursor.fetchone():
            logging.error(f"  FK Check Failed: Practitioner with practitioner_npi '{practitioner_npi}' not found in 'practitioners' table (for role).")
        else:
            logging.debug(f"  FK Check OK: Practitioner '{practitioner_npi}' found.")
     else:
         # This should not happen based on schema (NOT NULL), but check anyway
         logging.error(f"  FK Check Warning: Practitioner Role record has NULL practitioner_npi.")


     # hospital_fhir_id is nullable in practitioner_roles table
     if hospital_id:
        cursor.execute("SELECT 1 FROM hospitals WHERE hospital_fhir_id = ?", (hospital_id,))
        if not cursor.fetchone():
            logging.error(f"  FK Check Failed: Hospital with hospital_fhir_id '{hospital_id}' not found in 'hospitals' table (for role).")
        else:
            logging.debug(f"  FK Check OK: Hospital '{hospital_id}' found.")
     else:
        logging.debug(f"  FK Check Info: Role has NULL hospital_fhir_id (allowed).")


# --- Specific Insertion Functions (call the generic one or debug one) ---

def insert_patients(conn, patients, debug=False): # Add debug flag, default False
    """Inserts patient data."""
    columns = [
        'patient_fhir_id', 'first_name', 'middle_name', 'last_name', 'prefix',
        'mothers_maiden_name', 'dob', 'gender', 'marital_status', 'ssn',
        'drivers_license', 'passport', 'mrn', 'mrn_system', 'phone_home', 'address_line',
        'address_city', 'address_state', 'address_postal_code', 'address_country',
        'birth_city', 'birth_state', 'birth_country', 'language'
    ]
    if debug:
        # Generally not needed for primary tables, but possible
        logging.warning("Running patient insert in debug mode (row-by-row).")
        return insert_records_debug_mode(conn, 'patients', patients, columns)
    else:
        return insert_records(conn, 'patients', patients, columns)

def insert_hospitals(conn, hospitals, debug=False): # Add debug flag, default False
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

def insert_practitioners(conn, practitioners, debug=False): # Add debug flag, default False
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


def insert_practitioner_roles(conn, roles, debug=False): # Add debug flag
    """Inserts practitioner role data using the generic function or debug mode."""
    columns = [
        # Note: role_id is INTEGER PRIMARY KEY AUTOINCREMENT, so it's not included in the insert list
        'practitioner_npi', 'hospital_fhir_id', 'role_code', 'role_system', 'role_display',
        'specialty_code', 'specialty_system', 'specialty_display'
    ]
    if debug:
        # Call debug mode function which returns (success_count, failure_count)
        return insert_records_debug_mode(conn, 'practitioner_roles', roles, columns)
    else:
        # Call normal function which returns affected_rows (-1 on error)
        return insert_records(conn, 'practitioner_roles', roles, columns)

def insert_encounters(conn, encounters, debug=False): # Add debug flag
    """Inserts encounter data using the generic function or debug mode."""
    columns = [
        'encounter_id', 'patient_fhir_id', 'practitioner_npi', 'hospital_fhir_id',
        'start_time', 'end_time', 'encounter_class_code', 'encounter_type_code',
        'encounter_type_system', 'encounter_type_display'
    ]
    if debug:
        # Call debug mode function which returns (success_count, failure_count)
        return insert_records_debug_mode(conn, 'encounters', encounters, columns)
    else:
        # Call normal function which returns affected_rows (-1 on error)
        return insert_records(conn, 'encounters', encounters, columns)

# --- Main block for basic standalone testing ---
if __name__ == "__main__":
    # Use DEBUG level for standalone testing to see more details
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info("Running db_manager.py script directly for basic tests...")

    # Use an in-memory database for quick testing, or specify a file
    # test_db_name = ":memory:"
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
             # Add dummy insertion tests (use non-debug mode here)
             print("Testing dummy data insertion...")

             # Dummy Hospital (Parent)
             dummy_hospital = [{'hospital_fhir_id': 'test-hosp-1', 'name': 'Test Hospital'}]
             affected_hosp = insert_hospitals(conn, dummy_hospital) # Use non-debug
             print(f" Dummy Hospital Insert Result (affected rows): {affected_hosp}")

             # Dummy Practitioner (Parent)
             dummy_practitioner = [{'practitioner_npi': 'NPI-TEST-123', 'first_name': 'Test', 'last_name': 'Doctor'}]
             affected_prac = insert_practitioners(conn, dummy_practitioner) # Use non-debug
             print(f" Dummy Practitioner Insert Result (affected rows): {affected_prac}")

             # Dummy Patient (Parent)
             dummy_patient = [{'patient_fhir_id': 'test-pat-1', 'first_name': 'Dummy', 'last_name': 'Patient'}]
             affected_pat = insert_patients(conn, dummy_patient) # Use non-debug
             print(f" Dummy Patient Insert Result (affected rows): {affected_pat}")

             # Dummy Encounter (Child - should succeed)
             dummy_encounter_good = [{
                 'encounter_id': 'enc-good-1', 'patient_fhir_id': 'test-pat-1',
                 'practitioner_npi': 'NPI-TEST-123', 'hospital_fhir_id': 'test-hosp-1',
                 'start_time': '2023-01-01T10:00:00Z'
             }]
             print(" Testing valid encounter insertion (debug mode)...")
             # Test debug mode insertion for a valid record
             s_good, f_good = insert_encounters(conn, dummy_encounter_good, debug=True)
             print(f" Dummy Valid Encounter Insert Result (Success: {s_good}, Failed: {f_good})")
             if s_good == 1 and f_good == 0: print("  Valid Encounter Insert Test: SUCCESS")
             else: print("  Valid Encounter Insert Test: FAILED")


             # Dummy Encounter (Child - should fail FK constraint)
             dummy_encounter_bad_fk = [{
                 'encounter_id': 'enc-bad-fk-1', 'patient_fhir_id': 'test-pat-NONEXISTENT', # Bad Patient ID
                 'practitioner_npi': 'NPI-TEST-123', 'hospital_fhir_id': 'test-hosp-1',
                 'start_time': '2023-01-01T11:00:00Z'
             }]
             print(" Testing invalid encounter insertion (debug mode)...")
             # Test debug mode insertion for an invalid record
             s_bad, f_bad = insert_encounters(conn, dummy_encounter_bad_fk, debug=True)
             print(f" Dummy Invalid Encounter Insert Result (Success: {s_bad}, Failed: {f_bad})")
             if s_bad == 0 and f_bad == 1: print("  Invalid Encounter Insert Test (FK Fail): SUCCESS")
             else: print("  Invalid Encounter Insert Test (FK Fail): FAILED")

             # Test ignoring a duplicate patient (non-debug mode)
             print(" Testing duplicate patient insertion (should be ignored)...")
             affected_again = insert_patients(conn, dummy_patient) # Use non-debug
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