# fhir_processor/extract_hospitals.py
import json
import glob
import logging
import os

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
def extract_hospitals(hospital_file_pattern):
    """
    Extracts hospital data from FHIR bundle files manually.

    Returns:
        tuple: (list_of_hospital_dicts, hospital_lookup_map)
               The map uses synthea_id as key if available, otherwise fhir_id.
               Value is always the hospital_fhir_id.
    """
    hospital_files = glob.glob(hospital_file_pattern)
    hospitals_data = {} # Use dict keyed by fhir_id to ensure uniqueness
    hospital_lookup_map = {} # For mapping encounters later {lookup_key: fhir_id}
    logging.info(f"Found {len(hospital_files)} hospital information files matching pattern.")

    for file_path in hospital_files:
        logging.info(f"Processing hospital file: {os.path.basename(file_path)}")
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                bundle = json.load(f)

            for entry in _safe_get_internal(bundle, 'entry', []):
                resource = _safe_get_internal(entry, 'resource')
                if resource and _safe_get_internal(resource, 'resourceType') == 'Organization':

                    # --- Manual Parsing Logic ---
                    hospital_record = {}
                    try:
                        fhir_id = _safe_get_internal(resource, 'id')
                        if not fhir_id:
                            # Attempt to get from fullUrl if resource id is missing (less common in Org files from Synthea)
                            fhir_id = _safe_get_internal(entry, 'fullUrl')
                            if fhir_id and fhir_id.startswith('urn:uuid:'):
                                pass # Use the URN as ID
                            elif fhir_id and '/' in fhir_id:
                                fhir_id = fhir_id.split('/')[-1] # Extract ID part
                            else:
                                logging.warning(f"Could not determine FHIR ID for an Organization in {os.path.basename(file_path)}. Skipping resource.")
                                continue # Skip this resource if no ID found

                        hospital_record['hospital_fhir_id'] = fhir_id

                        # Synthea Identifier
                        synthea_id = None
                        identifiers = _safe_get_internal(resource, 'identifier', [])
                        for ident in identifiers:
                            if _safe_get_internal(ident, 'system') == 'https://github.com/synthetichealth/synthea':
                                synthea_id = _safe_get_internal(ident, 'value')
                                break
                        hospital_record['synthea_identifier'] = synthea_id

                        # Name
                        hospital_record['name'] = _safe_get_internal(resource, 'name')

                        # Address (taking the first one)
                        address_list = _safe_get_internal(resource, 'address', [])
                        if address_list:
                            addr = address_list[0]
                            line_list = _safe_get_internal(addr, 'line', [])
                            hospital_record['address_line'] = line_list[0] if line_list else None
                            hospital_record['address_city'] = _safe_get_internal(addr, 'city')
                            hospital_record['address_state'] = _safe_get_internal(addr, 'state')
                            hospital_record['address_postal_code'] = _safe_get_internal(addr, 'postalCode')
                            hospital_record['address_country'] = _safe_get_internal(addr, 'country')
                        else:
                            hospital_record['address_line'] = None
                            hospital_record['address_city'] = None
                            hospital_record['address_state'] = None
                            hospital_record['address_postal_code'] = None
                            hospital_record['address_country'] = None


                        # Telecom (taking the first phone number)
                        telecom_list = _safe_get_internal(resource, 'telecom', [])
                        phone_number = None
                        for telecom in telecom_list:
                            if _safe_get_internal(telecom, 'system') == 'phone':
                                phone_number = _safe_get_internal(telecom, 'value')
                                break # Take the first phone number found
                        hospital_record['phone'] = phone_number

                        # --- End Manual Parsing Logic ---

                        # Add to data and map, ensuring uniqueness by fhir_id
                        if fhir_id not in hospitals_data:
                            hospitals_data[fhir_id] = hospital_record
                            lookup_key = synthea_id if synthea_id else fhir_id
                            hospital_lookup_map[lookup_key] = fhir_id
                            if not synthea_id:
                                logging.warning(f"Hospital {fhir_id} ({hospital_record.get('name')}) missing Synthea identifier, mapping FHIR ID to itself in lookup.")
                        # else: Already processed this FHIR ID

                    except Exception as parse_error:
                        logging.error(f"Error parsing Organization resource in {os.path.basename(file_path)} (ID: {fhir_id if 'fhir_id' in locals() else 'UNKNOWN'}): {parse_error}", exc_info=True)


        except json.JSONDecodeError:
            logging.error(f"Error decoding JSON from file: {file_path}")
        except FileNotFoundError:
            logging.error(f"File not found: {file_path}")
        except Exception as e:
            logging.error(f"Error processing hospital file {file_path}: {e}", exc_info=True)

    logging.info(f"Extracted {len(hospitals_data)} unique hospital records.")
    # Return list of dictionary values for insertion and the map
    return list(hospitals_data.values()), hospital_lookup_map

# --- Main block for standalone execution/testing ---
if __name__ == "__main__":
    # Set up basic logging for standalone run
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info("Running extract_hospitals.py script directly (manual parsing)...")

    # Assume the script is run from the directory containing fhir_processor
    # Adjust if your execution context is different
    current_script_dir = os.path.dirname(os.path.abspath(__file__))
    fhir_output_dir = current_script_dir+"\\output\\fhir" # Go up one level

    test_hospital_pattern = os.path.join(fhir_output_dir, "hospitalInformation*.json")
    logging.info(f"Using pattern: {test_hospital_pattern}")

    extracted_hospitals, lookup_map = extract_hospitals(test_hospital_pattern)

    print("-" * 30)
    print(f"Extraction Summary:")
    print(f"  Total unique hospitals extracted: {len(extracted_hospitals)}")
    # Optional: Print some details of the first few hospitals
    if extracted_hospitals:
        print("\n  First few hospital records:")
        for i, hosp in enumerate(extracted_hospitals[:3]):
            print(f"    {i+1}. Name: {hosp.get('name')}")
            print(f"       FHIR ID: {hosp.get('hospital_fhir_id')}")
            print(f"       Synthea ID: {hosp.get('synthea_identifier')}")
            print(f"       Phone: {hosp.get('phone')}")
            print(f"       Address: {hosp.get('address_line')}, {hosp.get('address_city')}, {hosp.get('address_state')}")
    # Optional: Print lookup map size
    print(f"\n  Hospital Lookup Map Size: {len(lookup_map)}")
    if lookup_map:
        print("  Lookup Map Sample:", dict(list(lookup_map.items())[:3]))
    print("-" * 30)