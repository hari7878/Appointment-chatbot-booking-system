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
from extract_practitioners_and_roles import extract_practitioners_and_roles
from extract_patients import extract_patients_and_encounters
from db_manager import (
    create_connection, create_tables, insert_patients,
    insert_hospitals, insert_practitioners,
    insert_practitioner_roles, insert_encounters
)

# Configure logging - SET LEVEL TO DEBUG FOR DETAILED FOREIGN KEY CHECKS and PRE-CHECK WARNINGS
logging.basicConfig(
    level=logging.DEBUG,  # Use DEBUG during troubleshooting
    format='%(asctime)s - %(levelname)s [%(module)s:%(lineno)d] %(message)s' # Added line number
)

def main():
    """Main function to orchestrate the FHIR data processing and loading."""
    logging.info("="*10 + " Starting FHIR data processing " + "="*10)

    # --- Configuration Flag for Insertion Mode ---
    # Set to True to use row-by-row insertion for roles/encounters with detailed FK checks on failure
    # Set to False to use faster bulk insertion (executemany)
    # KEEP TRUE FOR NOW TO VERIFY THE FIX WORKS
    USE_DEBUG_INSERT_MODE = True # <-- CHANGE THIS TO False FOR NORMAL/FASTER RUNS AFTER VERIFICATION
    logging.info(f"Insertion mode: {'DEBUG (row-by-row)' if USE_DEBUG_INSERT_MODE else 'BULK (executemany)'}")

    # --- Deleting existing DB for a clean run (Optional - useful for testing) ---
    if os.path.exists(DATABASE_NAME):
        try:
            os.remove(DATABASE_NAME)
            logging.info(f"Removed existing database: {DATABASE_NAME}")
        except OSError as e:
            # Changed to warning as it might not be critical if removal fails
            logging.warning(f"Could not remove existing database {DATABASE_NAME}: {e}")
    # ---------------------------------------------------------------------------

    # --- Extraction Phase ---
    logging.info("--- Phase 1: Extracting Data from JSON Files ---")
    hospitals = []
    hospital_lookup_map = {}
    practitioners = []
    practitioner_roles = []
    patients = []
    encounters = []
    extraction_successful = True

    try:
        # Extract Hospitals first (no dependencies)
        hospitals, hospital_lookup_map = extract_hospitals(HOSPITAL_FILE_PATTERN)
        if not hospitals: logging.warning("No hospital data extracted.")
        else: logging.info(f"Extracted {len(hospitals)} unique hospitals.")

        # Extract Practitioners next (no dependencies)
        practitioners, practitioner_roles = extract_practitioners_and_roles(PRACTITIONER_FILE_PATTERN)
        if not practitioners: logging.warning("No practitioner data extracted.")
        else: logging.info(f"Extracted {len(practitioners)} unique practitioners and generated {len(practitioner_roles)} roles.")
        # Note: practitioner_roles are generated here but inserted later

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
             logging.warning("No patient bundle files identified after filtering. No patient or encounter data will be extracted.")
        else:
             # Pass the hospital map needed for resolving encounter service providers
             patients, encounters = extract_patients_and_encounters(patient_files_to_process, hospital_lookup_map)
             if not patients: logging.warning("No patient data extracted from identified files.")
             else: logging.info(f"Extracted {len(patients)} unique patients.")
             if not encounters: logging.warning("No encounter data extracted from patient files.")
             else: logging.info(f"Extracted {len(encounters)} unique encounters.")

    except Exception as e:
        logging.error(f"Critical error during data extraction phase: {e}", exc_info=True)
        extraction_successful = False # Mark extraction as failed

    if not extraction_successful:
        logging.error("Extraction failed. Aborting database loading.")
        return # Stop processing

    logging.info("--- Phase 1: Extraction Summary ---")
    logging.info(f" Hospitals: {len(hospitals)}")
    logging.info(f" Practitioners: {len(practitioners)}")
    logging.info(f" Patients: {len(patients)}")
    logging.info(f" Practitioner Roles (Generated): {len(practitioner_roles)}")
    logging.info(f" Encounters (Raw): {len(encounters)}") # Log raw count before check

    # --- Pre-Insertion Data Validation/Cleaning ---
    logging.info("--- Phase 1.5: Pre-checking Foreign Keys ---")
    # Create a set of valid NPIs from the extracted practitioners
    valid_npi_set = {p.get('practitioner_npi') for p in practitioners if p.get('practitioner_npi')}
    logging.debug(f"Built set of {len(valid_npi_set)} valid practitioner NPIs for checking.")

    # Check and modify encounters list for invalid practitioner NPIs
    encounters_to_insert = []
    nullified_npi_count = 0
    skipped_invalid_structure = 0 # Count records not in expected dict format

    for i, enc in enumerate(encounters):
        if not isinstance(enc, dict):
            logging.warning(f"Skipping encounter record at index {i} due to unexpected format: {type(enc)}")
            skipped_invalid_structure += 1
            continue

        npi = enc.get('practitioner_npi')
        enc_id = enc.get('encounter_id', 'UNKNOWN_ID') # Get ID for logging

        if npi and npi not in valid_npi_set:
            # This is the case identified in the previous run
            logging.warning(f"[FK Pre-Check] Encounter '{enc_id}' references missing NPI '{npi}'. Setting practitioner_npi to NULL.")
            enc['practitioner_npi'] = None # Set to NULL
            nullified_npi_count += 1
            encounters_to_insert.append(enc) # Add the modified record
        # elif not npi:
             # NPI is already NULL or empty, which is allowed by FK, keep it.
             # logging.debug(f"[FK Pre-Check] Encounter '{enc_id}' has NULL NPI (already valid).")
             # encounters_to_insert.append(enc)
        else:
            # NPI is present and valid, or already None/empty.
            # logging.debug(f"[FK Pre-Check] Encounter '{enc_id}' has valid NPI '{npi}' or is already NULL.")
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
            # Insert data (order matters due to foreign keys: Parents before Children)
            # Always use bulk insert for parent tables unless specifically debugging them.
            logging.info(f"Inserting {len(hospitals)} Hospitals...")
            hosp_res = insert_hospitals(conn, hospitals, debug=False)
            if hosp_res == -1: load_successful = False # Check if bulk insert failed

            if load_successful:
                logging.info(f"Inserting {len(practitioners)} Practitioners...")
                prac_res = insert_practitioners(conn, practitioners, debug=False)
                if prac_res == -1: load_successful = False

            if load_successful:
                logging.info(f"Inserting {len(patients)} Patients...")
                pat_res = insert_patients(conn, patients, debug=False)
                if pat_res == -1: load_successful = False

            # Insert child tables - Use the debug flag here
            if load_successful:
                logging.info(f"Inserting {len(practitioner_roles)} Practitioner Roles...")
                # Pass the debug flag; function handles return value based on mode
                # No pre-check was needed here as roles inserted fine previously
                role_res = insert_practitioner_roles(conn, practitioner_roles, debug=USE_DEBUG_INSERT_MODE)
                if USE_DEBUG_INSERT_MODE:
                    # Debug mode returns (success_count, failure_count)
                    s_roles, f_roles = role_res
                    logging.info(f"Practitioner Roles insertion results (Debug Mode) - Succeeded/Ignored: {s_roles}, Failed: {f_roles}")
                    if f_roles > 0:
                        load_successful = False # Mark failure if any roles failed in debug mode
                        logging.error(f"{f_roles} Practitioner Roles failed to insert.")
                elif role_res == -1: # Bulk mode returns -1 on error
                     load_successful = False
                     logging.error("Bulk insert failed for Practitioner Roles.")

            if load_successful:
                logging.info(f"Inserting {len(encounters_to_insert)} Encounters (post NPI check)...")
                # Use the potentially modified list: encounters_to_insert
                enc_res = insert_encounters(conn, encounters_to_insert, debug=USE_DEBUG_INSERT_MODE)
                if USE_DEBUG_INSERT_MODE:
                    # Debug mode returns (success_count, failure_count)
                    s_encs, f_encs = enc_res
                    logging.info(f"Encounters insertion results (Debug Mode) - Succeeded/Ignored: {s_encs}, Failed: {f_encs}")
                    if f_encs > 0:
                        load_successful = False # Mark failure if any encounters failed in debug mode
                        logging.error(f"{f_encs} Encounters failed to insert (check logs above for details).")
                elif enc_res == -1: # Bulk mode returns -1 on error
                     load_successful = False
                     logging.error("Bulk insert failed for Encounters.")

    except Exception as e:
        # Catch any unexpected errors during the loading sequence
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