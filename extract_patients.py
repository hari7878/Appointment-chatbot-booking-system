# fhir_processor/extract_patients.py
import json
import glob
import logging
import os
# Import necessary components for standalone test run
from config import HOSPITAL_FILE_PREFIX, PRACTITIONER_FILE_PREFIX
# We need extract_hospitals to get the map for encounter parsing in standalone mode
from extract_hospitals import extract_hospitals # Assuming this uses manual parsing now too

# --- Internal Helper Functions (Manual Equivalents) ---

def _safe_get_internal(data, keys, default=None):
    """
    Safely navigates nested dictionaries and lists.
    Internal helper for this script.
    """
    if not isinstance(keys, list):
        keys = [keys]
    temp = data
    for key in keys:
        try:
            if isinstance(temp, dict):
                temp = temp[key]
            elif isinstance(temp, list) and isinstance(key, int):
                temp = temp[key] # Access list element by index
            else:
                return default
        except (KeyError, IndexError, TypeError, AttributeError): # Handle various potential errors
            return default
    return temp



def _get_id_from_reference_internal(reference_str):
    """Internal helper: Extracts ID part from a FHIR reference string."""
    if not reference_str:
        return None
    if '/' in reference_str:
        return reference_str.split('/')[-1]
    # --- MODIFICATION START ---
    # Strip urn:uuid: prefix if present for consistent ID comparison
    if reference_str.startswith('urn:uuid:'):
        return reference_str[len('urn:uuid:'):]
    # --- MODIFICATION END ---
    # Fallback for other formats or plain IDs
    return reference_str.split('/')[-1]

def _get_npi_from_reference_internal(reference_str):
    """Internal helper: Extracts NPI from a Practitioner reference string."""
    if not reference_str or 'identifier=' not in reference_str or 'us-npi|' not in reference_str:
        return None
    try:
        parts = reference_str.split('identifier=')
        if len(parts) > 1:
            identifier_part = parts[1]
            if 'us-npi|' in identifier_part:
                npi = identifier_part.split('us-npi|')[1]
                npi = npi.split('&')[0].split(' ')[0]
                return npi.strip()
    except Exception as e:
        logging.warning(f"Could not parse NPI from reference '{reference_str}': {e}")
    return None

def _get_synthea_id_from_reference_internal(reference_str):
    """Internal helper: Extracts Synthea ID from an Organization reference string."""
    if not reference_str or 'identifier=' not in reference_str or 'synthea|' not in reference_str:
        return None
    try:
        parts = reference_str.split('identifier=')
        if len(parts) > 1:
            identifier_part = parts[1]
            if 'synthea|' in identifier_part:
                org_id = identifier_part.split('synthea|')[1]
                org_id = org_id.split('&')[0].split(' ')[0]
                return org_id.strip()
    except Exception as e:
        logging.warning(f"Could not parse Synthea Org ID from reference '{reference_str}': {e}")
    return None

def _get_identifier_internal(identifiers, system_uri=None, type_code=None, type_text=None):
    """Internal helper: Finds a specific identifier value from the list."""
    if not identifiers: return None
    for identifier in identifiers:
        match = True
        if system_uri and _safe_get_internal(identifier, 'system') != system_uri: match = False
        if match and type_code:
            type_data = _safe_get_internal(identifier, 'type', {})
            codings = _safe_get_internal(type_data, 'coding', [])
            if not any(_safe_get_internal(coding, 'code') == type_code for coding in codings): match = False
        if match and type_text and _safe_get_internal(identifier, ['type', 'text']) != type_text: match = False
        if match and 'value' in identifier: return identifier['value']
    return None

def _get_extension_value_internal(extensions, url):
    """Internal helper: Finds a specific extension value."""
    if not extensions: return None
    for extension in extensions:
        if _safe_get_internal(extension, 'url') == url:
            if 'valueString' in extension: return extension['valueString']
            if 'valueAddress' in extension: return extension['valueAddress']
            if 'valueDecimal' in extension: return extension['valueDecimal']
            return None
    return None

# --- Main Extraction Function ---
def extract_patients_and_encounters(list_of_patient_files, hospital_lookup_map):
    """
    Extracts patient and encounter data from a list of FHIR bundle file paths MANUALLY.

    Args:
        list_of_patient_files (list): A list of full paths to patient JSON files.
        hospital_lookup_map (dict): Map of {synthea_id or fhir_id: fhir_id} for hospitals.

    Returns:
        tuple: (list_of_patient_dicts, list_of_encounter_dicts)
    """
    patients_data = {} # Dict keyed by patient_fhir_id for uniqueness
    encounters_data = {} # Dict keyed by encounter_id for uniqueness
    logging.info(f"Processing {len(list_of_patient_files)} identified patient bundle files (manual extraction).")

    for file_path in list_of_patient_files:
        logging.debug(f"Processing patient file: {os.path.basename(file_path)}")
        current_patient_id = None
        patient_resource = None
        bundle_entries = [] # Store entries to iterate only once

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                bundle = json.load(f)

            if not isinstance(bundle, dict) or bundle.get('resourceType') != 'Bundle':
                logging.warning(f"File {os.path.basename(file_path)} is not a valid FHIR Bundle. Skipping.")
                continue

            bundle_entries = _safe_get_internal(bundle, 'entry', [])

            # Find the Patient resource first within the bundle
            for entry in bundle_entries:
                 resource = _safe_get_internal(entry, 'resource')
                 if resource and _safe_get_internal(resource, 'resourceType') == 'Patient':
                     patient_resource = resource
                     current_patient_id = _safe_get_internal(resource, 'id') or _get_id_from_reference_internal(_safe_get_internal(entry, 'fullUrl'))
                     break

            if not current_patient_id or not patient_resource:
                logging.warning(f"No Patient resource found or ID missing in {os.path.basename(file_path)}. Skipping file.")
                continue

            # --- Manual Patient Parsing ---
            if current_patient_id not in patients_data:
                patient_record = {'patient_fhir_id': current_patient_id}
                try:
                    # Name
                    name_list = _safe_get_internal(patient_resource, 'name', [])
                    if name_list:
                        official_name = next((n for n in name_list if _safe_get_internal(n, 'use') == 'official'), None)
                        name_to_parse = official_name if official_name else name_list[0]
                        given_names = _safe_get_internal(name_to_parse, 'given', [])
                        prefix_list = _safe_get_internal(name_to_parse, 'prefix', [])
                        patient_record['prefix'] = prefix_list[0] if prefix_list else None
                        patient_record['first_name'] = given_names[0] if given_names else None
                        patient_record['middle_name'] = given_names[1] if len(given_names) > 1 else None
                        patient_record['last_name'] = _safe_get_internal(name_to_parse, 'family')

                    # Extensions
                    extensions = _safe_get_internal(patient_resource, 'extension', [])
                    patient_record['mothers_maiden_name'] = _get_extension_value_internal(extensions,'http://hl7.org/fhir/StructureDefinition/patient-mothersMaidenName')
                    birth_address = _get_extension_value_internal(extensions, 'http://hl7.org/fhir/StructureDefinition/patient-birthPlace')
                    if isinstance(birth_address, dict):
                        patient_record['birth_city'] = _safe_get_internal(birth_address, 'city')
                        patient_record['birth_state'] = _safe_get_internal(birth_address, 'state')
                        patient_record['birth_country'] = _safe_get_internal(birth_address, 'country')

                    # Other direct fields
                    patient_record['dob'] = _safe_get_internal(patient_resource, 'birthDate')
                    patient_record['gender'] = _safe_get_internal(patient_resource, 'gender')
                    patient_record['marital_status'] = _safe_get_internal(patient_resource, ['maritalStatus', 'text']) or _safe_get_internal(patient_resource, ['maritalStatus', 'coding', 0, 'display'])

                    # Identifiers
                    identifiers = _safe_get_internal(patient_resource, 'identifier', [])
                    patient_record['ssn'] = _get_identifier_internal(identifiers, system_uri='http://hl7.org/fhir/sid/us-ssn')
                    patient_record['drivers_license'] = _get_identifier_internal(identifiers, type_code='DL')
                    patient_record['passport'] = _get_identifier_internal(identifiers, type_code='PPN')
                    patient_record['mrn'] = _get_identifier_internal(identifiers, type_code='MR')
                    patient_record['mrn_system'] = next((i['system'] for i in identifiers if _safe_get_internal(i, ['type', 'coding', 0, 'code']) == 'MR'), None)

                    # Telecom (Home Phone)
                    telecoms = _safe_get_internal(patient_resource, 'telecom', [])
                    phone_home = None
                    for tc in telecoms:
                        if _safe_get_internal(tc, 'system') == 'phone' and _safe_get_internal(tc, 'use') == 'home':
                            phone_home = _safe_get_internal(tc, 'value')
                            break
                    if not phone_home: # Fallback to first phone
                         for tc in telecoms:
                            if _safe_get_internal(tc, 'system') == 'phone':
                                phone_home = _safe_get_internal(tc, 'value')
                                break
                    patient_record['phone_home'] = phone_home

                    # Address
                    address_list = _safe_get_internal(patient_resource, 'address', [])
                    if address_list:
                        addr = address_list[0]
                        line_list = _safe_get_internal(addr, 'line', [])
                        patient_record['address_line'] = line_list[0] if line_list else None
                        patient_record['address_city'] = _safe_get_internal(addr, 'city')
                        patient_record['address_state'] = _safe_get_internal(addr, 'state')
                        patient_record['address_postal_code'] = _safe_get_internal(addr, 'postalCode')
                        patient_record['address_country'] = _safe_get_internal(addr, 'country')

                     # Language
                    communication = _safe_get_internal(patient_resource, 'communication', [])
                    lang_coding = _safe_get_internal(communication, [0, 'language', 'coding', 0])
                    patient_record['language'] = _safe_get_internal(lang_coding, 'code') or _safe_get_internal(communication, [0, 'language', 'text'])

                    # Add the parsed record
                    patients_data[current_patient_id] = patient_record

                except Exception as parse_error:
                     logging.error(f"Error manually parsing Patient resource {current_patient_id} in {os.path.basename(file_path)}: {parse_error}", exc_info=True)
                     continue # Skip encounters for this patient if parsing failed

            # --- Manual Encounter Parsing ---
            for entry in bundle_entries:
                resource = _safe_get_internal(entry, 'resource')
                if resource and _safe_get_internal(resource, 'resourceType') == 'Encounter':
                    encounter_record = {}
                    try:
                        encounter_id = _safe_get_internal(resource, 'id') or _get_id_from_reference_internal(_safe_get_internal(entry, 'fullUrl'))
                        if not encounter_id:
                            logging.warning(f"Encounter resource missing ID in {os.path.basename(file_path)}. Skipping.")
                            continue

                        # Check if encounter belongs to the bundle's patient
                        enc_patient_ref_id = _get_id_from_reference_internal(_safe_get_internal(resource, ['subject', 'reference']))
                        if enc_patient_ref_id != current_patient_id:
                             logging.warning(f"Encounter {encounter_id} in {os.path.basename(file_path)} references different patient ({enc_patient_ref_id}). Skipping.")
                             continue

                        # Skip if already processed this encounter ID
                        if encounter_id in encounters_data:
                            continue

                        encounter_record['encounter_id'] = encounter_id
                        encounter_record['patient_fhir_id'] = current_patient_id

                        # Practitioner NPI
                        participant = _safe_get_internal(resource, 'participant', [])
                        practitioner_ref = _safe_get_internal(participant, [0, 'individual', 'reference']) if participant else None
                        encounter_record['practitioner_npi'] = _get_npi_from_reference_internal(practitioner_ref)

                        # Hospital FHIR ID (using lookup map)
                        hospital_ref_str = _safe_get_internal(resource, ['serviceProvider', 'reference'])
                        hospital_fhir_id = None
                        if hospital_ref_str:
                            hospital_synthea_id = _get_synthea_id_from_reference_internal(hospital_ref_str)
                            if hospital_synthea_id and hospital_synthea_id in hospital_lookup_map:
                                hospital_fhir_id = hospital_lookup_map[hospital_synthea_id]
                            else:
                                direct_fhir_id = _get_id_from_reference_internal(hospital_ref_str)
                                if direct_fhir_id in hospital_lookup_map.values():
                                    hospital_fhir_id = direct_fhir_id
                                elif direct_fhir_id in hospital_lookup_map: # Check if mapped via FHIR ID key
                                     hospital_fhir_id = hospital_lookup_map[direct_fhir_id]
                                else:
                                     logging.warning(f"Could not map hospital reference '{hospital_ref_str}' to known hospital FHIR ID for encounter {encounter_id}")
                        encounter_record['hospital_fhir_id'] = hospital_fhir_id

                        # Other Encounter fields
                        encounter_record['start_time'] = _safe_get_internal(resource, ['period', 'start'])
                        encounter_record['end_time'] = _safe_get_internal(resource, ['period', 'end'])
                        encounter_record['encounter_class_code'] = _safe_get_internal(resource, ['class', 'code'])
                        encounter_record['encounter_type_code'] = _safe_get_internal(resource, ['type', 0, 'coding', 0, 'code'])
                        encounter_record['encounter_type_system'] = _safe_get_internal(resource, ['type', 0, 'coding', 0, 'system'])
                        encounter_record['encounter_type_display'] = _safe_get_internal(resource, ['type', 0, 'coding', 0, 'display']) or _safe_get_internal(resource, ['type', 0, 'text'])

                        # Add the parsed record
                        encounters_data[encounter_id] = encounter_record

                    except Exception as parse_error:
                        logging.error(f"Error manually parsing Encounter resource {encounter_id if 'encounter_id' in locals() else 'UNKNOWN'} in {os.path.basename(file_path)}: {parse_error}", exc_info=True)


        except json.JSONDecodeError:
            logging.error(f"Error decoding JSON from file: {file_path}")
        except FileNotFoundError:
            logging.error(f"File not found: {file_path}")
        except Exception as e:
            logging.error(f"Error processing patient file {file_path}: {e}", exc_info=True)

    logging.info(f"Extracted {len(patients_data)} unique patient records.")
    logging.info(f"Extracted {len(encounters_data)} unique encounter records.")
    return list(patients_data.values()), list(encounters_data.values())


# --- Main block for standalone execution/testing ---
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info("Running extract_patients.py script directly (manual parsing)...")

    current_dir = os.path.dirname(os.path.abspath(__file__))
    base_dir = os.path.dirname(current_dir)
    fhir_output_dir = current_dir+"\\output\\fhir" # Assumes script is run from fhir directory

    all_json_pattern = os.path.join(fhir_output_dir, "*.json")
    hospital_pattern = os.path.join(fhir_output_dir, f"{HOSPITAL_FILE_PREFIX}*.json")

    # Need the hospital map for encounter parsing even in standalone test
    logging.info("Extracting hospital map for encounter linking...")
    # Assuming extract_hospitals uses manual parsing now
    try:
        _, test_hospital_map = extract_hospitals(hospital_pattern)
        logging.info(f"Hospital map created with {len(test_hospital_map)} entries.")
    except Exception as e:
        logging.error(f"Error extracting hospital map (ensure extract_hospitals.py is updated and runnable): {e}")
        test_hospital_map = {}


    # Identify Patient Files by excluding hospital/practitioner info files
    all_files = glob.glob(all_json_pattern)
    test_patient_files = [
        f for f in all_files
        if not os.path.basename(f).startswith(HOSPITAL_FILE_PREFIX) and \
           not os.path.basename(f).startswith(PRACTITIONER_FILE_PREFIX)
    ]
    logging.info(f"Identified {len(test_patient_files)} potential patient files.")


    if not test_patient_files:
        logging.warning("No patient files found to process.")
    else:
        # Pass the identified list of files and the hospital map
        extracted_patients, extracted_encounters = extract_patients_and_encounters(test_patient_files, test_hospital_map)

        print("-" * 30)
        print(f"Extraction Summary:")
        print(f"  Total unique patients extracted: {len(extracted_patients)}")
        print(f"  Total unique encounters extracted: {len(extracted_encounters)}")

        if extracted_patients:
            print("\n  First few patient names:")
            for i, pat in enumerate(extracted_patients[:3]):
                 print(f"    {i+1}. {pat.get('prefix')} {pat.get('first_name')} {pat.get('last_name')} (ID: {pat.get('patient_fhir_id')})")

        if extracted_encounters:
            print("\n  First few encounter IDs / Types / Links:")
            for i, enc in enumerate(extracted_encounters[:5]):
                 print(f"    {i+1}. Encounter ID: {enc.get('encounter_id')}")
                 print(f"       Patient ID: {enc.get('patient_fhir_id')}")
                 print(f"       Type: {enc.get('encounter_type_display')} ({enc.get('encounter_type_code')})")
                 print(f"       Practitioner NPI: {enc.get('practitioner_npi')}")
                 print(f"       Hospital FHIR ID: {enc.get('hospital_fhir_id')}") # This is the mapped ID

        print("-" * 30)