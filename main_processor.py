# fhir_processor/main_processor.py
import os
import glob
import logging
from config import (
    ALL_JSON_PATTERN,
    HOSPITAL_FILE_PREFIX,
    PRACTITIONER_FILE_PREFIX,
    HOSPITAL_FILE_PATTERN,
    PRACTITIONER_FILE_PATTERN,
    DATABASE_NAME
)
from extract_hospitals import extract_hospitals
# Updated import for the modified practitioner extractor
from extract_practitioners_and_roles import extract_practitioners_schedules_slots
from extract_patients import extract_patients_and_encounters
from db_manager import (
    create_connection, create_tables, insert_patients,
    insert_hospitals, insert_practitioners,
    insert_practitioner_roles, insert_encounters,
    # Added new insertion functions
    insert_schedules, insert_slots
)

# Configure logging
logging.basicConfig(
    level=logging.DEBUG, # Keep DEBUG for detailed checks initially
    format='%(asctime)s - %(levelname)s [%(module)s:%(lineno)d] %(message)s'
)

def main():
    """Main function to orchestrate the FHIR data processing and loading."""
    logging.info("="*10 + " Starting FHIR data processing " + "="*10)

    # Flag for insertion mode (True for debug, False for bulk)
    USE_DEBUG_INSERT_MODE = False # Change to False for performance after testing
    logging.info(f"Insertion mode: {'DEBUG (row-by-row)' if USE_DEBUG_INSERT_MODE else 'BULK (executemany)'}")

    # Deleting existing DB for a clean run (Optional)
    if os.path.exists(DATABASE_NAME):
        try:
            os.remove(DATABASE_NAME)
            logging.info(f"Removed existing database: {DATABASE_NAME}")
        except OSError as e:
            logging.warning(f"Could not remove existing database {DATABASE_NAME}: {e}")

    # --- Extraction Phase ---
    logging.info("--- Phase 1: Extracting Data from JSON Files ---")
    hospitals = []
    hospital_lookup_map = {}
    practitioners = []
    practitioner_roles = []
    schedules = [] # New list for schedules
    slots = []     # New list for slots
    patients = []
    encounters = []
    extraction_successful = True

    try:
        # Extract Hospitals
        hospitals, hospital_lookup_map = extract_hospitals(HOSPITAL_FILE_PATTERN)
        if not hospitals: logging.warning("No hospital data extracted.")
        else: logging.info(f"Extracted {len(hospitals)} unique hospitals.")

        # Extract Practitioners, Roles, Schedules, and Slots
        # Note: practitioner_roles, schedules, slots are generated here but inserted later
        practitioners, practitioner_roles, schedules, slots = extract_practitioners_schedules_slots(PRACTITIONER_FILE_PATTERN)
        if not practitioners: logging.warning("No practitioner data extracted.")
        else: logging.info(f"Extracted {len(practitioners)} unique practitioners.")
        if not practitioner_roles: logging.warning("No practitioner roles generated.")
        else: logging.info(f"Generated {len(practitioner_roles)} roles.")
        if not schedules: logging.warning("No schedules generated.")
        else: logging.info(f"Generated {len(schedules)} schedules.")
        if not slots: logging.warning("No slots generated.")
        else: logging.info(f"Generated {len(slots)} slots.")


        # Identify and process Patient bundles (depend on hospital_lookup_map)
        all_json_files = glob.glob(ALL_JSON_PATTERN)
        logging.info(f"Found {len(all_json_files)} total JSON files matching pattern.")
        patient_files_to_process = [
            f for f in all_json_files
            if os.path.isfile(f) and \
               not os.path.basename(f).startswith(HOSPITAL_FILE_PREFIX) and \
               not os.path.basename(f).startswith(PRACTITIONER_FILE_PREFIX)
        ]
        logging.info(f"Identified {len(patient_files_to_process)} files potentially containing patient bundles.")

        if not patient_files_to_process:
             logging.warning("No patient bundle files identified. No patient or encounter data extracted.")
        else:
             patients, encounters = extract_patients_and_encounters(patient_files_to_process, hospital_lookup_map)
             if not patients: logging.warning("No patient data extracted.")
             else: logging.info(f"Extracted {len(patients)} unique patients.")
             if not encounters: logging.warning("No encounter data extracted.")
             else: logging.info(f"Extracted {len(encounters)} unique encounters.")

    except Exception as e:
        logging.error(f"Critical error during data extraction phase: {e}", exc_info=True)
        extraction_successful = False

    if not extraction_successful:
        logging.error("Extraction failed. Aborting database loading.")
        return

    logging.info("--- Phase 1: Extraction Summary ---")
    logging.info(f" Hospitals: {len(hospitals)}")
    logging.info(f" Practitioners: {len(practitioners)}")
    logging.info(f" Patients: {len(patients)}")
    logging.info(f" Practitioner Roles: {len(practitioner_roles)}")
    logging.info(f" Schedules: {len(schedules)}") # Added
    logging.info(f" Slots: {len(slots)}")         # Added
    logging.info(f" Encounters (Raw): {len(encounters)}")

    # --- Pre-Insertion Data Validation/Cleaning (Focus on Encounters NPI) ---
    logging.info("--- Phase 1.5: Pre-checking Foreign Keys for Encounters ---")
    valid_npi_set = {p.get('practitioner_npi') for p in practitioners if p.get('practitioner_npi')}
    logging.debug(f"Built set of {len(valid_npi_set)} valid practitioner NPIs for checking.")

    encounters_to_insert = []
    nullified_npi_count = 0
    skipped_invalid_structure = 0

    for i, enc in enumerate(encounters):
        if not isinstance(enc, dict):
            logging.warning(f"Skipping encounter record at index {i} due to unexpected format: {type(enc)}")
            skipped_invalid_structure += 1
            continue
        npi = enc.get('practitioner_npi')
        enc_id = enc.get('encounter_id', 'UNKNOWN_ID')
        if npi and npi not in valid_npi_set:
            logging.warning(f"[FK Pre-Check] Encounter '{enc_id}' references missing NPI '{npi}'. Setting practitioner_npi to NULL.")
            enc['practitioner_npi'] = None
            nullified_npi_count += 1
        encounters_to_insert.append(enc)

    if skipped_invalid_structure > 0:
         logging.warning(f"Skipped {skipped_invalid_structure} encounter records due to invalid format.")
    logging.info(f"Encounter pre-check complete. Nullified NPIs for {nullified_npi_count} encounters.")
    logging.info(f"Total encounters prepared for insertion: {len(encounters_to_insert)}")

    # --- Database Loading Phase ---
    logging.info("--- Phase 2: Loading Data into SQLite Database ---")
    conn = create_connection(DATABASE_NAME)
    if conn is None:
        logging.error("Failed to create database connection. Exiting.")
        return

    load_successful = True
    try:
        if not create_tables(conn):
             logging.error("Failed to create database tables. Aborting loading.")
             load_successful = False
        else:
            # --- Correct Insertion Order based on Dependencies ---
            # 1. Parent Tables (no FKs to tables within this list)
            logging.info(f"Inserting {len(hospitals)} Hospitals...")
            hosp_res = insert_hospitals(conn, hospitals, debug=False)
            if hosp_res == -1: load_successful = False

            if load_successful:
                logging.info(f"Inserting {len(practitioners)} Practitioners...")
                prac_res = insert_practitioners(conn, practitioners, debug=False)
                if prac_res == -1: load_successful = False

            if load_successful:
                logging.info(f"Inserting {len(patients)} Patients...")
                pat_res = insert_patients(conn, patients, debug=False)
                if pat_res == -1: load_successful = False

            # 2. Child Tables (depend on parents inserted above)
            # Schedules depend on Practitioners
            if load_successful:
                logging.info(f"Inserting {len(schedules)} Schedules...")
                sch_res = insert_schedules(conn, schedules, debug=USE_DEBUG_INSERT_MODE)
                if USE_DEBUG_INSERT_MODE:
                    s_sch, f_sch = sch_res
                    logging.info(f"Schedules insertion results (Debug Mode) - Succeeded/Ignored: {s_sch}, Failed: {f_sch}")
                    if f_sch > 0: load_successful = False; logging.error(f"{f_sch} Schedules failed.")
                elif sch_res == -1: load_successful = False; logging.error("Bulk insert failed for Schedules.")

            # PractitionerRoles depend on Practitioners, Hospitals
            if load_successful:
                logging.info(f"Inserting {len(practitioner_roles)} Practitioner Roles...")
                role_res = insert_practitioner_roles(conn, practitioner_roles, debug=USE_DEBUG_INSERT_MODE)
                if USE_DEBUG_INSERT_MODE:
                    s_roles, f_roles = role_res
                    logging.info(f"Practitioner Roles results (Debug Mode) - Succeeded/Ignored: {s_roles}, Failed: {f_roles}")
                    if f_roles > 0: load_successful = False; logging.error(f"{f_roles} Roles failed.")
                elif role_res == -1: load_successful = False; logging.error("Bulk insert failed for Roles.")

            # Encounters depend on Patients, Practitioners, Hospitals
            if load_successful:
                logging.info(f"Inserting {len(encounters_to_insert)} Encounters (post NPI check)...")
                enc_res = insert_encounters(conn, encounters_to_insert, debug=USE_DEBUG_INSERT_MODE)
                if USE_DEBUG_INSERT_MODE:
                    s_encs, f_encs = enc_res
                    logging.info(f"Encounters results (Debug Mode) - Succeeded/Ignored: {s_encs}, Failed: {f_encs}")
                    if f_encs > 0: load_successful = False; logging.error(f"{f_encs} Encounters failed.")
                elif enc_res == -1: load_successful = False; logging.error("Bulk insert failed for Encounters.")

            # Slots depend on Schedules
            if load_successful:
                logging.info(f"Inserting {len(slots)} Slots...")
                slot_res = insert_slots(conn, slots, debug=USE_DEBUG_INSERT_MODE)
                if USE_DEBUG_INSERT_MODE:
                    s_slots, f_slots = slot_res
                    logging.info(f"Slots insertion results (Debug Mode) - Succeeded/Ignored: {s_slots}, Failed: {f_slots}")
                    if f_slots > 0: load_successful = False; logging.error(f"{f_slots} Slots failed.")
                elif slot_res == -1: load_successful = False; logging.error("Bulk insert failed for Slots.")

            # --- End Correct Insertion Order ---

    except Exception as e:
        logging.error(f"Unexpected error during database loading phase: {e}", exc_info=True)
        load_successful = False
    finally:
        if conn:
            conn.close()
            logging.info("Database connection closed.")

    # --- Final Summary ---
    logging.info("--- Phase 2: Database Loading Complete ---")
    if load_successful:
        logging.info("="*10 + " FHIR data processing finished successfully " + "="*10)
    else:
        logging.error("="*10 + " FHIR data processing finished with errors during loading " + "="*10)


if __name__ == "__main__":
    main()
