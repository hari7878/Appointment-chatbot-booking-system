# fhir_processor/extract_practitioners.py
import json
import glob
import logging
import os
import random  # Import the random module

# Import role definitions directly from config
from config import SPECIALTY_TO_ROLE_CODE, DEFAULT_ROLE, SNOMED_SYSTEM, HL7_ROLE_SYSTEM

# --- Helper Function (Manual Equivalent of safe_get) ---
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

# --- Main Extraction Function ---
def extract_practitioners_and_roles(practitioner_file_pattern):
    """
    Extracts practitioner data from FHIR bundle files manually and assigns
    a random specialized role to each unique practitioner.

    Returns:
        tuple: (list_of_practitioner_dicts, list_of_role_dicts)
    """
    practitioner_files = glob.glob(practitioner_file_pattern)
    practitioners_data = {} # Use dict keyed by NPI for uniqueness
    practitioner_roles_data = [] # List to store assigned roles
    logging.info(f"Found {len(practitioner_files)} practitioner information files matching pattern.")

    # --- Prepare list of potential roles for random assignment ---
    # Exclude the default 'doctor' role if you only want specialized ones
    # Or include it if 'doctor' is an acceptable random assignment
    possible_roles = [
        role_info for spec, role_info in SPECIALTY_TO_ROLE_CODE.items()
        if role_info[0] != DEFAULT_ROLE[0] # Exclude the default 'doctor' role
    ]
    if not possible_roles:
        logging.warning("No specialized roles found in config map for random assignment. Using default 'doctor' role.")
        # Fallback to using the default role if the specialized list is empty
        possible_roles = [DEFAULT_ROLE]


    for file_path in practitioner_files:
        logging.info(f"Processing practitioner file: {os.path.basename(file_path)}")
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                bundle = json.load(f)

            for entry in _safe_get_internal(bundle, 'entry', []):
                resource = _safe_get_internal(entry, 'resource')
                if resource and _safe_get_internal(resource, 'resourceType') == 'Practitioner':

                    # --- Manual Parsing Logic ---
                    practitioner_record = {}
                    try:
                        # NPI (Primary Key)
                        npi = None
                        identifiers = _safe_get_internal(resource, 'identifier', [])
                        for ident in identifiers:
                            if _safe_get_internal(ident, 'system') == 'http://hl7.org/fhir/sid/us-npi':
                                npi = _safe_get_internal(ident, 'value')
                                break
                        if not npi:
                            fhir_id = _safe_get_internal(resource, 'id')
                            logging.warning(f"Practitioner resource {fhir_id} in {os.path.basename(file_path)} missing NPI. Skipping.")
                            continue # Skip if no NPI

                        practitioner_record['practitioner_npi'] = npi

                        # Skip if already processed this NPI
                        if npi in practitioners_data:
                            # logging.debug(f"Skipping already processed practitioner NPI: {npi}")
                            continue

                        # Name
                        name_list = _safe_get_internal(resource, 'name', [])
                        if name_list:
                            name_to_parse = name_list[0] # Take the first name entry
                            given_names = _safe_get_internal(name_to_parse, 'given', [])
                            prefix_list = _safe_get_internal(name_to_parse, 'prefix', [])
                            practitioner_record['prefix'] = prefix_list[0] if prefix_list else None
                            practitioner_record['first_name'] = given_names[0] if given_names else None
                            practitioner_record['last_name'] = _safe_get_internal(name_to_parse, 'family')
                        else:
                            practitioner_record['prefix'] = None
                            practitioner_record['first_name'] = None
                            practitioner_record['last_name'] = None

                        # Gender
                        practitioner_record['gender'] = _safe_get_internal(resource, 'gender')

                        # Address
                        address_list = _safe_get_internal(resource, 'address', [])
                        if address_list:
                            addr = address_list[0]
                            line_list = _safe_get_internal(addr, 'line', [])
                            practitioner_record['address_line'] = line_list[0] if line_list else None
                            practitioner_record['address_city'] = _safe_get_internal(addr, 'city')
                            practitioner_record['address_state'] = _safe_get_internal(addr, 'state')
                            practitioner_record['address_postal_code'] = _safe_get_internal(addr, 'postalCode')
                            practitioner_record['address_country'] = _safe_get_internal(addr, 'country')
                        else:
                            practitioner_record['address_line'] = None
                            practitioner_record['address_city'] = None
                            practitioner_record['address_state'] = None
                            practitioner_record['address_postal_code'] = None
                            practitioner_record['address_country'] = None


                        # Telecom (Email)
                        telecom_list = _safe_get_internal(resource, 'telecom', [])
                        email = None
                        for telecom in telecom_list:
                            if _safe_get_internal(telecom, 'system') == 'email':
                                email = _safe_get_internal(telecom, 'value')
                                break
                        practitioner_record['email'] = email

                        # --- Add to practitioner list ---
                        practitioners_data[npi] = practitioner_record # Add to dict using NPI as key

                        # --- Assign Random Role ---
                        if possible_roles: # Ensure we have roles to choose from
                            chosen_role_code, chosen_role_system, chosen_role_display = random.choice(possible_roles)

                            # Determine specialty based on the assigned role
                            is_snomed_role = chosen_role_system == SNOMED_SYSTEM
                            specialty_code = chosen_role_code if is_snomed_role else None
                            specialty_system = chosen_role_system if is_snomed_role else None
                            specialty_display = chosen_role_display if is_snomed_role else None


                            role_record = {
                                'practitioner_npi': npi,
                                'hospital_fhir_id': None, # Role not tied to a specific hospital in this context
                                'role_code': chosen_role_code,
                                'role_system': chosen_role_system,
                                'role_display': chosen_role_display,
                                'specialty_code': specialty_code,
                                'specialty_system': specialty_system,
                                'specialty_display': specialty_display
                            }
                            practitioner_roles_data.append(role_record)
                            logging.debug(f"Assigned random role '{chosen_role_display}' to NPI {npi}")
                        else:
                             logging.warning(f"Could not assign random role to NPI {npi} as possible_roles list is empty.")


                    except Exception as parse_error:
                        logging.error(f"Error parsing Practitioner resource in {os.path.basename(file_path)} (NPI: {npi if 'npi' in locals() else 'UNKNOWN'}): {parse_error}", exc_info=True)

        except json.JSONDecodeError:
            logging.error(f"Error decoding JSON from file: {file_path}")
        except FileNotFoundError:
            logging.error(f"File not found: {file_path}")
        except Exception as e:
            logging.error(f"Error processing practitioner file {file_path}: {e}", exc_info=True)

    logging.info(f"Extracted {len(practitioners_data)} unique practitioner records.")
    logging.info(f"Assigned {len(practitioner_roles_data)} random roles.")
    return list(practitioners_data.values()), practitioner_roles_data

# --- Main block for standalone execution/testing ---
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info("Running extract_practitioners.py script directly (manual parsing + random roles)...")

    current_dir = os.path.dirname(os.path.abspath(__file__))
    base_dir = os.path.dirname(current_dir)
    fhir_output_dir = current_dir +"\\output\\fhir"

    test_practitioner_pattern = os.path.join(fhir_output_dir, "practitionerInformation*.json")
    logging.info(f"Using pattern: {test_practitioner_pattern}")

    extracted_practitioners, assigned_roles = extract_practitioners_and_roles(test_practitioner_pattern)

    print("-" * 30)
    print(f"Extraction Summary:")
    print(f"  Total unique practitioners extracted: {len(extracted_practitioners)}")
    print(f"  Total random roles assigned: {len(assigned_roles)}")

    if extracted_practitioners:
        print("\n  First few practitioner names/NPIs:")
        for i, prac in enumerate(extracted_practitioners[:3]):
            print(f"    {i+1}. {prac.get('prefix')} {prac.get('first_name')} {prac.get('last_name')} (NPI: {prac.get('practitioner_npi')})")

    if assigned_roles:
        print("\n  First few assigned roles:")
        # Find roles corresponding to the first few practitioners
        npis_to_show = [p.get('practitioner_npi') for p in extracted_practitioners[:3]]
        shown_roles = 0
        for role in assigned_roles:
            if role.get('practitioner_npi') in npis_to_show and shown_roles < 5:
                 print(f"    - NPI: {role.get('practitioner_npi')}, Role: {role.get('role_display')} ({role.get('role_code')}), Specialty: {role.get('specialty_display')}")
                 shown_roles += 1
            elif shown_roles >= 5:
                 break

    print("-" * 30)