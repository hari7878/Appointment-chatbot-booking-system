# tools/search_tools.py
import logging
import sqlite3
from datetime import datetime, timedelta, date
from typing import Optional, List, Dict, Any
from pydantic.v1 import BaseModel, Field
from langchain_core.tools import tool
import json # Import json for printing dicts

# Setup for relative imports and config loading
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Use relative import for utils within the tools package
try:
    # Try relative import first
    from chatbot.tools.tool_utils import create_db_connection, get_specialty_map
except ImportError: # Handle running script directly
    from tool_utils import create_db_connection, get_specialty_map


# Import config constants
try:
    from chatbot.config import MAX_SLOT_SUGGESTIONS_PER_DAY, SUGGESTION_WINDOW_DAYS
except ImportError:
    MAX_SLOT_SUGGESTIONS_PER_DAY = 3
    SUGGESTION_WINDOW_DAYS = 7
    print("Warning: tools/search_tools.py: Could not import chatbot_config.")

logger = logging.getLogger(__name__)
# Ensure logging is configured
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s [%(filename)s:%(lineno)d] %(message)s')


# --- Input Schemas ---
class FindDoctorsByExactTermsInput(BaseModel):
    db_specialty_terms: List[str] = Field(description="A list of *exact* specialty or role display names (validated from the database) to search for doctors.")

class FindMoreSlotsInput(BaseModel):
    practitioner_npi: str = Field(description="The NPI (National Provider Identifier) of the specific doctor.")
    start_date: Optional[str] = Field(None, description="Optional specific date (YYYY-MM-DD) to start searching from. Defaults to today.")

class FindSpecificAppointmentInput(BaseModel):
    patient_fhir_id: str = Field(description="The FHIR ID of the patient whose appointment needs to be found.")
    appointment_info: str = Field(description="Information to help identify the appointment, like the approximate time ('Tomorrow morning', '2024-08-15 around 2 PM'), doctor's name ('Dr. Smith'), or specialty ('Cardiology appointment').")

class GetPatientAppointmentsInput(BaseModel):
    patient_fhir_id: str = Field(description="The FHIR ID of the patient whose appointments should be retrieved.")

# --- Tools (Implementations as provided in previous answers) ---
@tool("find_doctors_and_initial_slots", args_schema=FindDoctorsByExactTermsInput)
def find_doctors_and_initial_slots(db_specialty_terms: List[str]) -> Dict[str, Any]:
    """
    Finds doctors based on a list of EXACT, VALIDATED specialty/role display names from the database.
    Provides a few upcoming available slots for each doctor found.
    *** DO NOT call this directly with user input. Use 'validate_specialty_term' first. ***
    Returns lists of doctors with their NPIs and a sample of their free slots (including Slot IDs).
    """
    logger.info(f"Tool 'find_doctors_and_initial_slots' called with EXACT terms: {db_specialty_terms}")
    conn = None
    doctors_with_slots = []
    if not db_specialty_terms:
        return {"status": "error", "message": "Internal error: No specialty terms provided for doctor search."}
    try:
        conn = create_db_connection()
        cursor = conn.cursor()
        specialty_map = get_specialty_map() # Warm up cache if needed
        placeholders = ', '.join('?' * len(db_specialty_terms))
        query_npi = f"""
            SELECT DISTINCT practitioner_npi
            FROM practitioner_roles
            WHERE specialty_display IN ({placeholders}) OR role_display IN ({placeholders})
        """
        cursor.execute(query_npi, db_specialty_terms * 2)
        matching_npis = [row['practitioner_npi'] for row in cursor.fetchall()]
        if not matching_npis:
            return {"status": "no_doctors_found", "message": f"Although the specialty was recognized, I couldn't find any doctors currently listed under '{', '.join(db_specialty_terms)}'."}
        logger.info(f"Found {len(matching_npis)} NPIs for exact terms. Getting details and slots...")
        npis_to_process = matching_npis[:5]
        start_date_dt = date.today()
        end_date_dt = start_date_dt + timedelta(days=SUGGESTION_WINDOW_DAYS)
        start_date_str = start_date_dt.strftime('%Y-%m-%d')
        end_date_str = end_date_dt.strftime('%Y-%m-%d')

        for npi in npis_to_process:
            cursor.execute("SELECT first_name, last_name FROM practitioners WHERE practitioner_npi = ?", (npi,))
            practitioner_details = cursor.fetchone()
            if not practitioner_details: continue
            cursor.execute("SELECT schedule_fhir_id FROM schedules WHERE practitioner_npi = ? AND active = 1", (npi,))
            schedule_row = cursor.fetchone()
            if not schedule_row: continue
            schedule_id = schedule_row['schedule_fhir_id']
            doctor_name = f"{practitioner_details['first_name']} {practitioner_details['last_name']}"
            slots_query = """
                SELECT slot_fhir_id, start_time, end_time
                FROM slots
                WHERE schedule_fhir_id = ? AND status = 'free' AND DATE(start_time) >= ? AND DATE(start_time) < ?
                ORDER BY start_time LIMIT ?;
            """
            limit = MAX_SLOT_SUGGESTIONS_PER_DAY
            cursor.execute(slots_query, (schedule_id, start_date_str, end_date_str, limit))
            slots_found = [dict(row) for row in cursor.fetchall()]

            has_more_slots = False
            if len(slots_found) == limit:
                more_slots_check_query = "SELECT 1 FROM slots WHERE schedule_fhir_id = ? AND status = 'free' AND DATE(start_time) >= ? AND DATE(start_time) < ? LIMIT 1 OFFSET ?;"
                cursor.execute(more_slots_check_query, (schedule_id, start_date_str, end_date_str, limit))
                if cursor.fetchone(): has_more_slots = True

            if not slots_found: continue

            formatted_slots = [f"Slot ID: {s['slot_fhir_id']} @ {s['start_time']}" for s in slots_found]
            cursor.execute("SELECT DISTINCT role_display, specialty_display FROM practitioner_roles WHERE practitioner_npi = ?", (npi,))
            role_data = cursor.fetchone()
            display_specialty = (role_data['specialty_display'] or role_data['role_display'] or 'Unknown') if role_data else 'Unknown'
            doctors_with_slots.append({
                "name": doctor_name, "npi": npi, "display_specialty": display_specialty,
                "slots_preview": formatted_slots, "has_more_slots": has_more_slots
            })
        if not doctors_with_slots:
            return {"status": "no_slots_found", "message": f"I found doctors for '{', '.join(db_specialty_terms)}', but none seem to have available slots in the next {SUGGESTION_WINDOW_DAYS} days."}
        logger.info(f"Returning initial slots for {len(doctors_with_slots)} doctors for terms: {db_specialty_terms}.")
        return {"status": "success", "message": f"Okay, for '{', '.join(db_specialty_terms)}', I found these doctors with upcoming availability:", "doctors": doctors_with_slots}
    except sqlite3.Error as e:
        logger.error(f"Database error in find_doctors_and_initial_slots: {e}")
        return {"status": "error", "message": "Sorry, I encountered a database error searching for doctors."}
    except Exception as e:
        logger.error(f"Unexpected error in find_doctors_and_initial_slots: {e}", exc_info=True)
        return {"status": "error", "message": "An unexpected error occurred searching for doctors."}
    finally:
        if conn: conn.close()

@tool("find_more_available_slots", args_schema=FindMoreSlotsInput)
def find_more_available_slots(practitioner_npi: str, start_date: Optional[str] = None) -> Dict[str, Any]:
    """
    Finds ALL available ('free') time slots for a specific practitioner starting from today or a specified date.
    Use this when the user asks for a specific doctor's full schedule or needs more options than the initial preview.
    Returns a list of available slots including their Slot IDs.
    """
    logger.info(f"Tool 'find_more_available_slots' called for NPI: {practitioner_npi}, Start Date: {start_date}")
    conn = None
    try:
        search_start_date = date.today()
        if start_date:
            try:
                search_start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
            except ValueError:
                logger.warning(f"Invalid start_date format: {start_date}. Using today.")
        search_end_date = search_start_date + timedelta(weeks=4)
        search_start_date_str = search_start_date.strftime('%Y-%m-%d')
        search_end_date_str = search_end_date.strftime('%Y-%m-%d')
        logger.debug(f"Searching slots for {practitioner_npi} between {search_start_date_str} and {search_end_date_str}")
        conn = create_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT schedule_fhir_id FROM schedules WHERE practitioner_npi = ? AND active = 1", (practitioner_npi,))
        schedule_row = cursor.fetchone()
        if not schedule_row:
            return {"status": "no_schedule", "message": f"I couldn't find an active schedule for the doctor with NPI {practitioner_npi}."}
        schedule_id = schedule_row['schedule_fhir_id']
        query = """
            SELECT slot_fhir_id, start_time, end_time
            FROM slots
            WHERE schedule_fhir_id = ?
              AND status = 'free'
              AND DATE(start_time) >= ? AND DATE(start_time) < ?
            ORDER BY start_time;
        """
        cursor.execute(query, (schedule_id, search_start_date_str, search_end_date_str))
        all_slots = [dict(row) for row in cursor.fetchall()]
        if not all_slots:
            return {"status": "no_slots_found", "message": f"Sorry, I couldn't find any available slots for this doctor between {search_start_date_str} and {search_end_date_str}."}
        formatted_slots = [f"Slot ID: {s['slot_fhir_id']} :: Time: {s['start_time']} to {s['end_time']}" for s in all_slots]
        return {"status": "success", "message": f"Here are the available slots I found starting from {search_start_date_str}:", "slots": formatted_slots, "raw_slots": all_slots}
    except sqlite3.Error as e:
        logger.error(f"Database error in find_more_available_slots: {e}")
        return {"status": "error", "message": "A database error occurred."}
    except Exception as e:
        logger.error(f"Unexpected error in find_more_available_slots: {e}", exc_info=True)
        return {"status": "error", "message": "An unexpected error occurred."}
    finally:
        if conn: conn.close()

@tool("find_specific_appointment", args_schema=FindSpecificAppointmentInput)
def find_specific_appointment(patient_fhir_id: str, appointment_info: str) -> Dict[str, Any]:
    """
    Finds a specific appointment for a patient based on provided info (time, doctor name, etc.).
    Used before updating or cancelling to confirm the correct appointment.
    If multiple appointments match vaguely, asks the user to clarify by providing the Slot ID.
    """
    logger.info(f"Tool 'find_specific_appointment' called for Patient: {patient_fhir_id}, Info: '{appointment_info}'")
    conn = None
    try:
        conn = create_db_connection()
        cursor = conn.cursor()
        # Base query
        query = """
            SELECT
                a.appointment_id, a.slot_fhir_id, sl.start_time, sl.end_time,
                p.first_name || ' ' || p.last_name as practitioner_name,
                p.practitioner_npi, pr.role_display, pr.specialty_display
            FROM appointments a
            JOIN slots sl ON a.slot_fhir_id = sl.slot_fhir_id
            JOIN schedules s ON sl.schedule_fhir_id = s.schedule_fhir_id
            JOIN practitioners p ON s.practitioner_npi = p.practitioner_npi
            LEFT JOIN practitioner_roles pr ON p.practitioner_npi = pr.practitioner_npi
            WHERE a.patient_fhir_id = ? AND a.status = 'confirmed'
        """
        params = [patient_fhir_id]
        # Split user info into potential keywords
        info_parts = appointment_info.lower().split()
        clauses = []
        potential_slot_id = None

        # Check if info looks like a slot_id
        for part in info_parts:
            if 'slot-' in part and len(part) > 15:
                 potential_slot_id = part
                 logger.debug(f"Potential slot ID found in info: {potential_slot_id}")
                 break

        if potential_slot_id:
            # If it looks like a slot ID, search specifically by that first
             logger.debug(f"Searching specifically for slot ID: {potential_slot_id}")
             query += " AND a.slot_fhir_id = ?"
             params.append(potential_slot_id)
        else:
            # If not a slot ID, perform broader keyword search
            logger.debug("No slot ID detected, performing keyword search on appointment info.")
            for part in info_parts:
                term = f"%{part}%"
                # Match against time (partially), practitioner name, role/specialty
                clauses.append("(LOWER(sl.start_time) LIKE ? OR LOWER(p.first_name) LIKE ? OR LOWER(p.last_name) LIKE ? OR LOWER(pr.role_display) LIKE ? OR LOWER(pr.specialty_display) LIKE ?)")
                params.extend([term] * 5) # Add term 5 times, once for each LIKE

            if clauses:
                # Use AND to require all keywords to match *something* in the record
                query += " AND (" + " AND ".join(clauses) + ")"

        query += " ORDER BY sl.start_time;"
        logger.debug(f"Executing appointment search query: {query} with params: {params}")

        cursor.execute(query, tuple(params))
        matching_appointments = [dict(row) for row in cursor.fetchall()]

        # --- Process results ---
        if not matching_appointments:
            return {"status": "not_found", "message": "I couldn't find any confirmed appointments matching that description."}
        elif len(matching_appointments) == 1:
            appt = matching_appointments[0]
            formatted = f"Slot ID: {appt['slot_fhir_id']} with {appt['practitioner_name']} from {appt['start_time']} to {appt['end_time']}"
            return {"status": "found_specific", "message": f"Okay, I found this appointment: {formatted}. Is this the one you want to modify/cancel?", "appointment_details": appt}
        else:
            # Found multiple possibilities
            formatted_options = [f"Slot ID: {appt['slot_fhir_id']} :: Doctor: {appt['practitioner_name']} :: Time: {appt['start_time']}" for appt in matching_appointments]
            return {"status": "found_multiple", "message": "I found a few appointments that might match. Which one are you referring to? Please provide the Slot ID:", "possible_appointments": formatted_options, "raw_appointments": matching_appointments}

    except sqlite3.Error as e:
        logger.error(f"Database error in find_specific_appointment: {e}")
        return {"status": "error", "message": "A database error occurred while searching for your appointment."}
    except Exception as e:
        logger.error(f"Unexpected error in find_specific_appointment: {e}", exc_info=True)
        return {"status": "error", "message": "An unexpected error occurred while searching."}
    finally:
        if conn: conn.close()


@tool("get_patient_appointments", args_schema=GetPatientAppointmentsInput)
def get_patient_appointments(patient_fhir_id: str) -> Dict[str, Any]:
    """
    Retrieves all confirmed appointments for a given patient.
    """
    logger.info(f"Tool 'get_patient_appointments' called for Patient: {patient_fhir_id}")
    conn = None
    try:
        conn = create_db_connection()
        cursor = conn.cursor()
        query = """
            SELECT a.appointment_id, a.slot_fhir_id, sl.start_time, sl.end_time,
                   p.first_name || ' ' || p.last_name as practitioner_name
            FROM appointments a
            JOIN slots sl ON a.slot_fhir_id = sl.slot_fhir_id
            JOIN schedules s ON sl.schedule_fhir_id = s.schedule_fhir_id
            JOIN practitioners p ON s.practitioner_npi = p.practitioner_npi
            WHERE a.patient_fhir_id = ? AND a.status = 'confirmed' ORDER BY sl.start_time;
        """
        cursor.execute(query, (patient_fhir_id,))
        appointments = [dict(row) for row in cursor.fetchall()]
        if not appointments:
            return {"status": "not_found", "message": "You don't have any upcoming confirmed appointments.", "appointments": []}
        else:
            formatted = [f"Slot ID: {a['slot_fhir_id']} with {a['practitioner_name']} from {a['start_time']} to {a['end_time']}" for a in appointments]
            return {"status": "success", "message": "Here are your upcoming appointments:", "appointments": formatted, "raw_appointments": appointments}
    except sqlite3.Error as e:
        logger.error(f"Database error in get_patient_appointments: {e}")
        return {"status": "error", "message": "A database error occurred."}
    except Exception as e:
        logger.error(f"Unexpected error in get_patient_appointments: {e}", exc_info=True)
        return {"status": "error", "message": "An unexpected error occurred."}
    finally:
        if conn: conn.close()


# --- Test Block ---
if __name__ == "__main__":
    print("--- Testing search_tools.py ---")
    # Set logging to DEBUG for detailed query view during testing
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger(__name__).setLevel(logging.DEBUG) # Ensure this module's logger is also DEBUG

    # Assumes DB exists and has data. These values might need adjusting based on your DB.
    test_validated_terms = ["Cardiologist"] # Example term after validation
    test_npi = "9999899799" # Example NPI for Cardiology
    test_patient = "1f497115-11b3-6ee8-d508-9360e220db37" # Example patient ID
    # Choose a slot ID that *should* exist and be FREE in your test DB for setup
    slot_to_make_busy = "slot-sch-999998-ee8ed08333bd" # Example free slot for Dr. Teran

    conn_test = None
    try:
        # --- Setup: Insert a dummy appointment for testing find/get ---
        print("\n--- Setting up dummy appointment for testing ---")
        conn_test = create_db_connection()
        cursor_test = conn_test.cursor()
        # Ensure the slot exists and is free first
        cursor_test.execute("SELECT status FROM slots WHERE slot_fhir_id = ?", (slot_to_make_busy,))
        slot_status_row = cursor_test.fetchone()
        if not slot_status_row:
            print(f"ERROR: Slot {slot_to_make_busy} does not exist in the database. Cannot run tests.")
            exit()
        elif slot_status_row['status'] != 'free':
            print(f"WARNING: Slot {slot_to_make_busy} is not 'free' ({slot_status_row['status']}). Attempting to force free for test.")
            cursor_test.execute("UPDATE slots SET status = 'free' WHERE slot_fhir_id = ?", (slot_to_make_busy,))
            # Ensure any previous appointment for this slot is removed
            cursor_test.execute("DELETE FROM appointments WHERE slot_fhir_id = ?", (slot_to_make_busy,))

        # Insert the dummy appointment
        print(f"Inserting dummy appointment for patient {test_patient} and slot {slot_to_make_busy}...")
        cursor_test.execute("INSERT INTO appointments (patient_fhir_id, slot_fhir_id, status) VALUES (?, ?, ?)",
                            (test_patient, slot_to_make_busy, 'confirmed'))
        # Mark the slot as busy
        cursor_test.execute("UPDATE slots SET status = 'busy' WHERE slot_fhir_id = ?", (slot_to_make_busy,))
        conn_test.commit()
        print("Dummy appointment setup complete.")
        # --- End Setup ---

        # --- Run Tests ---
        print(f"\nTesting find_doctors_and_initial_slots with validated terms: {test_validated_terms}")
        try:
            result = find_doctors_and_initial_slots.invoke({"db_specialty_terms": test_validated_terms})
            print(json.dumps(result, indent=2))
        except Exception as e:
            print(f"Error: {e}")

        print(f"\nTesting find_more_available_slots for NPI: {test_npi}")
        try:
            result = find_more_available_slots.invoke({"practitioner_npi": test_npi})
            print(json.dumps(result, indent=2))
        except Exception as e:
            print(f"Error: {e}")

        print(f"\nTesting find_specific_appointment for patient {test_patient}")
        # Test cases for find_specific_appointment - now using the booked slot
        test_infos = [
            slot_to_make_busy, # Search by the specific booked ID
            'Gabriel934 Terán294',    # Search by doctor name and time hint (adjust if doctor name differs)
            "nonexistent appointment info"
        ]
        for info in test_infos:
            print(f"\nSearching with info: '{info}'")
            try:
                result = find_specific_appointment.invoke({"patient_fhir_id": test_patient, "appointment_info": info})
                print(json.dumps(result, indent=2))
                # Basic assertion for the specific ID case
                if info == slot_to_make_busy:
                    assert result.get("status") == "found_specific"
                    assert result.get("appointment_details", {}).get("slot_fhir_id") == slot_to_make_busy
            except Exception as e:
                print(f"Error: {e}")

        print(f"\nTesting get_patient_appointments for patient {test_patient}")
        try:
            result = get_patient_appointments.invoke({"patient_fhir_id": test_patient})
            print(json.dumps(result, indent=2))
            # Basic assertion: Check if the booked slot is present
            assert result.get("status") == "success"
            found = any(appt.get("slot_fhir_id") == slot_to_make_busy for appt in result.get("raw_appointments", []))
            assert found, f"Booked appointment {slot_to_make_busy} not found in get_patient_appointments result."
            print("-> Get patient appointments test passed.")
        except Exception as e:
            print(f"Error: {e}")
            assert False, "get_patient_appointments test failed"

    except Exception as e_outer:
         print(f"Outer error during testing: {e_outer}")
    finally:
        # --- Teardown: Clean up the dummy appointment ---
        if conn_test:
            print("\n--- Cleaning up dummy appointment ---")
            try:
                cursor_test = conn_test.cursor()
                # Delete the appointment
                cursor_test.execute("DELETE FROM appointments WHERE patient_fhir_id = ? AND slot_fhir_id = ?",
                                    (test_patient, slot_to_make_busy))
                # Mark the slot as free again
                cursor_test.execute("UPDATE slots SET status = 'free' WHERE slot_fhir_id = ?", (slot_to_make_busy,))
                conn_test.commit()
                print(f"Dummy appointment for slot {slot_to_make_busy} removed and slot marked free.")
            except Exception as e_cleanup:
                print(f"Error during cleanup: {e_cleanup}")
            finally:
                 conn_test.close()
                 print("Test database connection closed.")
        # --- End Teardown ---

        print("\n--- Testing search_tools.py Complete ---")