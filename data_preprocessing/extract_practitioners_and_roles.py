# fhir_processor/extract_practitioners_schedule_slots.py
import json
import glob
import logging
import os
import random
import uuid # For generating unique IDs
from datetime import datetime, timedelta, time

# Import role definitions and schedule config directly from config
from data_preprocessing.config import (
    SPECIALTY_TO_ROLE_CODE, DEFAULT_ROLE, SNOMED_SYSTEM, HL7_ROLE_SYSTEM,
    SCHEDULE_HORIZON_DAYS, SLOT_WORKING_DAY_START_HOUR, SLOT_WORKING_DAY_END_HOUR,
    SLOT_DURATIONS_MINUTES, SLOT_STATUS_CHOICES, SLOT_STATUS_WEIGHTS,
    format_datetime_for_db # Import the helper
)

# --- Helper Function (Manual Equivalent of safe_get) ---
def _safe_get_internal(data, keys, default=None):
    """Safely navigates nested dictionaries and lists."""
    if not isinstance(keys, list): keys = [keys]
    temp = data
    for key in keys:
        try:
            if isinstance(temp, dict): temp = temp[key]
            elif isinstance(temp, list) and isinstance(key, int): temp = temp[key]
            else: return default
        except (KeyError, IndexError, TypeError, AttributeError): return default
    return temp

# --- Main Extraction and Generation Function ---
def extract_practitioners_schedules_slots(practitioner_file_pattern):
    """
    Extracts practitioner data, assigns random roles, and generates
    corresponding schedule and slot data.

    Returns:
        tuple: (list_of_practitioner_dicts, list_of_role_dicts,
                list_of_schedule_dicts, list_of_slot_dicts)
    """
    practitioner_files = glob.glob(practitioner_file_pattern)
    practitioners_data = {} # Use dict keyed by NPI for uniqueness
    practitioner_roles_data = []
    schedules_data = []
    slots_data = []

    logging.info(f"Found {len(practitioner_files)} practitioner information files matching pattern.")

    # Prepare list of potential roles for random assignment
    possible_roles = [
        role_info for spec, role_info in SPECIALTY_TO_ROLE_CODE.items()
        if role_info[0] != DEFAULT_ROLE[0] # Exclude default 'doctor'
    ]
    if not possible_roles:
        logging.warning("No specialized roles found for random assignment. Using default 'doctor' role.")
        possible_roles = [DEFAULT_ROLE]

    # Get today's date to calculate future schedule horizon
    today = datetime.utcnow().date()
    # Find the next Monday (or today if it is Monday)
    start_date = today + timedelta(days=(0 - today.weekday() + 7) % 7)
    end_date = start_date + timedelta(days=SCHEDULE_HORIZON_DAYS)

    for file_path in practitioner_files:
        logging.info(f"Processing practitioner file: {os.path.basename(file_path)}")
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                bundle = json.load(f)

            for entry in _safe_get_internal(bundle, 'entry', []):
                resource = _safe_get_internal(entry, 'resource')
                if resource and _safe_get_internal(resource, 'resourceType') == 'Practitioner':
                    # --- Manual Practitioner Parsing ---
                    practitioner_record = {}
                    try:
                        npi = None
                        identifiers = _safe_get_internal(resource, 'identifier', [])
                        for ident in identifiers:
                            if _safe_get_internal(ident, 'system') == 'http://hl7.org/fhir/sid/us-npi':
                                npi = _safe_get_internal(ident, 'value')
                                break
                        if not npi:
                            fhir_id = _safe_get_internal(resource, 'id')
                            logging.warning(f"Practitioner resource {fhir_id} missing NPI. Skipping.")
                            continue

                        # Skip if already processed this NPI
                        if npi in practitioners_data:
                            continue

                        practitioner_record['practitioner_npi'] = npi

                        # Name
                        name_list = _safe_get_internal(resource, 'name', [])
                        if name_list:
                            name_to_parse = name_list[0]
                            given_names = _safe_get_internal(name_to_parse, 'given', [])
                            prefix_list = _safe_get_internal(name_to_parse, 'prefix', [])
                            practitioner_record['prefix'] = prefix_list[0] if prefix_list else None
                            practitioner_record['first_name'] = given_names[0] if given_names else None
                            practitioner_record['last_name'] = _safe_get_internal(name_to_parse, 'family')
                        else:
                            practitioner_record['prefix'] = None
                            practitioner_record['first_name'] = None
                            practitioner_record['last_name'] = None
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
                        else: # Simplified - set all to None if address missing
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
                        practitioners_data[npi] = practitioner_record

                        # --- Assign Random Role ---
                        if possible_roles:
                            chosen_role_code, chosen_role_system, chosen_role_display = random.choice(possible_roles)
                            is_snomed_role = chosen_role_system == SNOMED_SYSTEM
                            specialty_code = chosen_role_code if is_snomed_role else None
                            specialty_system = chosen_role_system if is_snomed_role else None
                            specialty_display = chosen_role_display if is_snomed_role else None

                            role_record = {
                                'practitioner_npi': npi,
                                'hospital_fhir_id': None, # Role not tied to specific hospital here
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
                            logging.warning(f"Could not assign role to NPI {npi}.")


                        # --- Generate Schedule for this Practitioner ---
                        schedule_fhir_id = f"sch-{npi[:8]}-{uuid.uuid4().hex[:12]}" # Create a unique ID
                        # Combine date with start/end time for full datetime objects
                        horizon_start_dt = datetime.combine(start_date, time.min) # Start of the first day
                        horizon_end_dt = datetime.combine(end_date, time.max) # End of the last day

                        schedule_record = {
                            'schedule_fhir_id': schedule_fhir_id,
                            'practitioner_npi': npi,
                            'active': 1, # Default to active
                            'planning_horizon_start': format_datetime_for_db(horizon_start_dt),
                            'planning_horizon_end': format_datetime_for_db(horizon_end_dt),
                            'comment': f"Availability schedule for {practitioner_record.get('first_name', '')} {practitioner_record.get('last_name', '')}"
                        }
                        schedules_data.append(schedule_record)
                        logging.debug(f"Generated schedule {schedule_fhir_id} for NPI {npi}")


                        # --- Generate Slots for this Schedule ---
                        current_date = start_date
                        while current_date <= end_date:
                            # Generate slots only for weekdays (Monday=0, Sunday=6)
                            if current_date.weekday() < 5:
                                day_start_time = datetime.combine(current_date, time(SLOT_WORKING_DAY_START_HOUR))
                                day_end_time = datetime.combine(current_date, time(SLOT_WORKING_DAY_END_HOUR))
                                current_slot_start_time = day_start_time

                                while current_slot_start_time < day_end_time:
                                    # Choose random duration
                                    duration_minutes = random.choice(SLOT_DURATIONS_MINUTES)
                                    current_slot_end_time = current_slot_start_time + timedelta(minutes=duration_minutes)

                                    # Ensure slot doesn't exceed working hours
                                    if current_slot_end_time > day_end_time:
                                        break # Stop generating slots for this day

                                    # Choose status based on weights
                                    slot_status = random.choices(SLOT_STATUS_CHOICES, weights=SLOT_STATUS_WEIGHTS, k=1)[0]
                                    slot_fhir_id = f"slot-{schedule_fhir_id[:10]}-{uuid.uuid4().hex[:12]}"

                                    slot_record = {
                                        'slot_fhir_id': slot_fhir_id,
                                        'schedule_fhir_id': schedule_fhir_id,
                                        'status': slot_status,
                                        'start_time': format_datetime_for_db(current_slot_start_time),
                                        'end_time': format_datetime_for_db(current_slot_end_time),
                                        'comment': None # No specific comment for generated slots
                                    }
                                    slots_data.append(slot_record)

                                    # Move to the next slot start time
                                    current_slot_start_time = current_slot_end_time

                            # Move to the next day
                            current_date += timedelta(days=1)
                        logging.debug(f"Generated slots for schedule {schedule_fhir_id}")

                    except Exception as parse_error:
                        logging.error(f"Error parsing Practitioner/generating schedule/slots in {os.path.basename(file_path)} (NPI: {npi if 'npi' in locals() else 'UNKNOWN'}): {parse_error}", exc_info=True)

        except json.JSONDecodeError:
            logging.error(f"Error decoding JSON from file: {file_path}")
        except FileNotFoundError:
            logging.error(f"File not found: {file_path}")
        except Exception as e:
            logging.error(f"Error processing practitioner file {file_path}: {e}", exc_info=True)

    logging.info(f"Extracted {len(practitioners_data)} unique practitioner records.")
    logging.info(f"Assigned {len(practitioner_roles_data)} random roles.")
    logging.info(f"Generated {len(schedules_data)} schedule records.")
    logging.info(f"Generated {len(slots_data)} slot records.")
    return list(practitioners_data.values()), practitioner_roles_data, schedules_data, slots_data

# --- Main block for standalone execution/testing ---
if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s') # Use DEBUG for more details
    logging.info("Running extract_practitioners_schedule_slots.py script directly...")
    # Assume script is run from directory containing fhir_processor
    current_script_dir = os.path.dirname(os.path.abspath(__file__))
    fhir_output_dir = os.path.join(os.path.dirname(current_script_dir), 'data_preprocessing', 'output', 'fhir') # Adjust path as needed

    test_practitioner_pattern = os.path.join(fhir_output_dir, "practitionerInformation*.json")
    logging.info(f"Using pattern: {test_practitioner_pattern}")

    if not glob.glob(test_practitioner_pattern):
         logging.error(f"No files found matching pattern: {test_practitioner_pattern}")
         logging.error("Please ensure Synthea FHIR output exists at the specified location.")
    else:
        extracted_practs, assigned_roles, generated_schedules, generated_slots = extract_practitioners_schedules_slots(test_practitioner_pattern)

        print("-" * 30)
        print(f"Extraction Summary:")
        print(f"  Practitioners extracted: {len(extracted_practs)}")
        print(f"  Roles assigned: {len(assigned_roles)}")
        print(f"  Schedules generated: {len(generated_schedules)}")
        print(f"  Slots generated: {len(generated_slots)}")

        if extracted_practs:
            print("\n  First few practitioner names/NPIs:")
            for i, prac in enumerate(extracted_practs[:3]):
                print(f"    {i+1}. {prac.get('prefix')} {prac.get('first_name')} {prac.get('last_name')} (NPI: {prac.get('practitioner_npi')})")

        if generated_schedules:
            print("\n  First few generated schedules:")
            for i, sch in enumerate(generated_schedules[:3]):
                 print(f"    {i+1}. Schedule ID: {sch.get('schedule_fhir_id')}")
                 print(f"       Practitioner NPI: {sch.get('practitioner_npi')}")
                 print(f"       Horizon: {sch.get('planning_horizon_start')} -> {sch.get('planning_horizon_end')}")

        if generated_slots:
            print("\n  First few generated slots:")
            for i, slot in enumerate(generated_slots[:5]):
                 print(f"    {i+1}. Slot ID: {slot.get('slot_fhir_id')}")
                 print(f"       Schedule ID: {slot.get('schedule_fhir_id')}")
                 print(f"       Time: {slot.get('start_time')} -> {slot.get('end_time')}")
                 print(f"       Status: {slot.get('status')}")

        print("-" * 30)