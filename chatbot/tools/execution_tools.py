# tools/execution_tools.py
import logging
import sqlite3
from typing import Dict, Any
from pydantic.v1 import BaseModel, Field
from langchain_core.tools import tool

# Setup for relative imports and config loading
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Use relative import for utils within the tools package
from chatbot.tools.tool_utils import create_db_connection

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s [%(filename)s:%(lineno)d] %(message)s')

# --- Input Schemas ---
class ExecuteBookingInput(BaseModel):
    patient_fhir_id: str = Field(description="The FHIR ID of the patient booking the appointment.")
    slot_fhir_id: str = Field(description="The unique FHIR ID of the specific time slot the patient has confirmed they want to book.")

class ExecuteUpdateInput(BaseModel):
    patient_fhir_id: str = Field(description="The FHIR ID of the patient updating the appointment.")
    old_slot_fhir_id: str = Field(description="The FHIR ID of the currently booked slot that the user confirmed they want to change.")
    new_slot_fhir_id: str = Field(description="The FHIR ID of the new free slot that the user confirmed they want to book instead.")

class ExecuteCancellationInput(BaseModel):
    patient_fhir_id: str = Field(description="The FHIR ID of the patient cancelling the appointment.")
    slot_fhir_id_to_cancel: str = Field(description="The FHIR ID of the specific appointment slot the user confirmed they want to cancel.")

# --- Tools ---
@tool("execute_booking", args_schema=ExecuteBookingInput)
def execute_booking(patient_fhir_id: str, slot_fhir_id: str) -> Dict[str, Any]:
    """
    Executes the booking of a specific time slot for a patient *after* the user has confirmed.
    Verifies the slot is still available and updates the database atomically.
    """
    # ... (Implementation as before) ...
    logger.info(f"Tool 'execute_booking' called for Patient: {patient_fhir_id}, Slot: {slot_fhir_id}")
    conn = None
    try:
        conn = create_db_connection()
        cursor = conn.cursor()
        conn.execute("BEGIN TRANSACTION;")
        cursor.execute("SELECT schedule_fhir_id, start_time, end_time FROM slots WHERE slot_fhir_id = ? AND status = 'free'", (slot_fhir_id,))
        slot_data = cursor.fetchone()
        if not slot_data:
            cursor.execute("SELECT status FROM slots WHERE slot_fhir_id = ?", (slot_fhir_id,))
            existing_slot = cursor.fetchone()
            conn.rollback()
            status = "conflict" if existing_slot else "not_found"
            message = "Sorry, that time slot is no longer available. Please try finding slots again." if status == "conflict" else "Sorry, I couldn't find that specific time slot anymore."
            logger.warning(f"Booking execution failed for slot {slot_fhir_id}. Status: {status}")
            return {"status": status, "message": message}
        slot_details = dict(slot_data)
        cursor.execute("UPDATE slots SET status = 'busy' WHERE slot_fhir_id = ?", (slot_fhir_id,))
        if cursor.rowcount == 0: conn.rollback(); return {"status": "error", "message": "Internal error: Failed to update slot status."}
        try:
            cursor.execute("INSERT INTO appointments (patient_fhir_id, slot_fhir_id, status) VALUES (?, ?, ?)", (patient_fhir_id, slot_fhir_id, 'confirmed'))
            appointment_id = cursor.lastrowid
        except sqlite3.IntegrityError as ie:
             conn.rollback()
             logger.warning(f"Booking execution conflict (IntegrityError likely UNIQUE) for slot {slot_fhir_id}: {ie}")
             return {"status": "conflict", "message": "It seems this slot was booked just now by someone else."}
        except Exception as e_inner:
             conn.rollback()
             logger.error(f"Error inserting appointment for slot {slot_fhir_id}: {e_inner}", exc_info=True)
             return {"status": "error", "message": "An error occurred while recording the appointment."}
        conn.commit()
        logger.info(f"Booking executed: Appointment {appointment_id} created for patient {patient_fhir_id}, slot {slot_fhir_id}.")
        cursor.execute("""SELECT p.first_name || ' ' || p.last_name as practitioner_name FROM practitioners p JOIN schedules s ON p.practitioner_npi = s.practitioner_npi WHERE s.schedule_fhir_id = ?""", (slot_details['schedule_fhir_id'],))
        practitioner_info = cursor.fetchone(); doc_name = dict(practitioner_info).get('practitioner_name', 'the doctor') if practitioner_info else 'the doctor'
        return {"status": "success", "message": f"Appointment confirmed! You are booked with {doc_name} from {slot_details['start_time']} to {slot_details['end_time']}."}
    except sqlite3.Error as e:
        if conn: conn.rollback(); logger.error(f"Database error in execute_booking: {e}")
        return {"status": "error", "message": "A database error occurred during booking execution."}
    except Exception as e:
        if conn: conn.rollback(); logger.error(f"Unexpected error in execute_booking: {e}", exc_info=True)
        return {"status": "error", "message": "An unexpected error occurred during booking."}
    finally:
        if conn: conn.close()


@tool("execute_update", args_schema=ExecuteUpdateInput)
def execute_update(patient_fhir_id: str, old_slot_fhir_id: str, new_slot_fhir_id: str) -> Dict[str, Any]:
    """
    Executes the update of an appointment *after* the user has confirmed the old and new slots.
    Performs checks and database modifications atomically.
    """
    # ... (Implementation as before) ...
    logger.info(f"Tool 'execute_update' called for Patient: {patient_fhir_id}, Old Slot: {old_slot_fhir_id}, New Slot: {new_slot_fhir_id}")
    conn = None
    if old_slot_fhir_id == new_slot_fhir_id: return {"status": "no_change", "message": "No update needed."}
    try:
        conn = create_db_connection()
        cursor = conn.cursor()
        conn.execute("BEGIN TRANSACTION;")
        cursor.execute("SELECT appointment_id FROM appointments WHERE patient_fhir_id = ? AND slot_fhir_id = ? AND status = 'confirmed'", (patient_fhir_id, old_slot_fhir_id))
        old_appt = cursor.fetchone()
        if not old_appt: conn.rollback(); return {"status": "not_found_old", "message": f"Could not find the original appointment (Slot ID: {old_slot_fhir_id}) to update."}
        old_appointment_id = old_appt['appointment_id']
        cursor.execute("SELECT schedule_fhir_id, start_time, end_time FROM slots WHERE slot_fhir_id = ? AND status = 'free'", (new_slot_fhir_id,))
        new_slot_data = cursor.fetchone()
        if not new_slot_data: conn.rollback(); return {"status": "conflict_new", "message": f"The new time slot (ID: {new_slot_fhir_id}) is not available."}
        new_slot_details = dict(new_slot_data)
        cursor.execute("UPDATE appointments SET slot_fhir_id = ?, last_updated = CURRENT_TIMESTAMP WHERE appointment_id = ?", (new_slot_fhir_id, old_appointment_id))
        if cursor.rowcount == 0: conn.rollback(); return {"status": "error", "message": "Internal error: Failed to update appointment record."}
        cursor.execute("UPDATE slots SET status = 'free' WHERE slot_fhir_id = ?", (old_slot_fhir_id,))
        if cursor.rowcount == 0: logger.error(f"CRITICAL: Failed to free old slot {old_slot_fhir_id} during update of appointment {old_appointment_id}.")
        cursor.execute("UPDATE slots SET status = 'busy' WHERE slot_fhir_id = ?", (new_slot_fhir_id,))
        if cursor.rowcount == 0: conn.rollback(); return {"status": "error", "message": "Internal error: Failed to secure the new slot."}
        conn.commit()
        logger.info(f"Update executed: Appointment {old_appointment_id} updated to slot {new_slot_fhir_id} for patient {patient_fhir_id}.")
        cursor.execute("""SELECT p.first_name || ' ' || p.last_name as practitioner_name FROM practitioners p JOIN schedules s ON p.practitioner_npi = s.practitioner_npi WHERE s.schedule_fhir_id = ?""", (new_slot_details['schedule_fhir_id'],))
        practitioner_info = cursor.fetchone(); doc_name = dict(practitioner_info).get('practitioner_name', 'the doctor') if practitioner_info else 'the doctor'
        return {"status": "success", "message": f"Appointment updated successfully! Your new time with {doc_name} is from {new_slot_details['start_time']} to {new_slot_details['end_time']}."}
    except sqlite3.Error as e:
        if conn: conn.rollback(); logger.error(f"Database error in execute_update: {e}")
        return {"status": "error", "message": "A database error occurred during update execution."}
    except Exception as e:
        if conn: conn.rollback(); logger.error(f"Unexpected error in execute_update: {e}", exc_info=True)
        return {"status": "error", "message": "An unexpected error occurred during update."}
    finally:
        if conn: conn.close()

@tool("execute_cancellation", args_schema=ExecuteCancellationInput)
def execute_cancellation(patient_fhir_id: str, slot_fhir_id_to_cancel: str) -> Dict[str, Any]:
    """
    Executes the cancellation of a specific appointment *after* the user has confirmed.
    Updates the database atomically.
    """
    # ... (Implementation as before) ...
    logger.info(f"Tool 'execute_cancellation' called for Patient: {patient_fhir_id}, Slot: {slot_fhir_id_to_cancel}")
    conn = None
    try:
        conn = create_db_connection()
        cursor = conn.cursor()
        conn.execute("BEGIN TRANSACTION;")
        cursor.execute("SELECT appointment_id, start_time, end_time FROM appointments a JOIN slots s ON a.slot_fhir_id = s.slot_fhir_id WHERE a.patient_fhir_id = ? AND a.slot_fhir_id = ? AND a.status = 'confirmed'", (patient_fhir_id, slot_fhir_id_to_cancel))
        appointment_to_cancel = cursor.fetchone()
        if not appointment_to_cancel: conn.rollback(); return {"status": "not_found", "message": f"Could not find the appointment (Slot ID: {slot_fhir_id_to_cancel}) to cancel."}
        appt_details = dict(appointment_to_cancel); appointment_id = appt_details['appointment_id']
        cursor.execute("DELETE FROM appointments WHERE appointment_id = ?", (appointment_id,))
        if cursor.rowcount == 0: conn.rollback(); return {"status": "error", "message": "Internal error: Failed to delete appointment record."}
        cursor.execute("UPDATE slots SET status = 'free' WHERE slot_fhir_id = ?", (slot_fhir_id_to_cancel,))
        if cursor.rowcount == 0:
            conn.commit()
            logger.error(f"CRITICAL: Failed to free slot {slot_fhir_id_to_cancel} after deleting appointment {appointment_id}.")
            return {"status": "success_with_warning", "message": f"Appointment for slot {slot_fhir_id_to_cancel} cancelled, but there was an issue freeing the slot status."}
        conn.commit()
        logger.info(f"Cancellation executed: Appointment {appointment_id} (Slot: {slot_fhir_id_to_cancel}) cancelled for patient {patient_fhir_id}.")
        return {"status": "success", "message": f"Your appointment (Slot: {slot_fhir_id_to_cancel}, Time: {appt_details['start_time']} to {appt_details['end_time']}) has been cancelled."}
    except sqlite3.Error as e:
        if conn: conn.rollback(); logger.error(f"Database error in execute_cancellation: {e}")
        return {"status": "error", "message": "A database error occurred during cancellation."}
    except Exception as e:
        if conn: conn.rollback(); logger.error(f"Unexpected error in execute_cancellation: {e}", exc_info=True)
        return {"status": "error", "message": "An unexpected error occurred."}
    finally:
        if conn: conn.close()

# --- Test Block ---
if __name__ == "__main__":
    import json
    print("--- Testing execution_tools.py ---")
    logging.basicConfig(level=logging.DEBUG)

    # These tests modify the database state.
    # Ideally, use a dedicated test DB and reset between tests.
    # For this example, we assume the DB state allows these actions.

    test_patient = "1f497115-11b3-6ee8-d508-9360e220db37" # Replace with actual Patient FHIR ID from your DB
    # Find a free slot ID from your DB for booking test
    free_slot_for_booking = "slot-sch-999998-255f75dd5216" # Replace with a known FREE slot ID
    # Find a slot ID currently booked by test_patient for cancel/update tests
    booked_slot_by_patient = "slot-sch-999998-255f75dd5216" # Replace if needed (setup_test_db booked this)
    # Find another free slot ID for the update test
    free_slot_for_update = "slot-sch-999998-ee8ed08333bd" # Replace with a known FREE slot ID

    print(f"\nTesting execute_booking for patient {test_patient}, slot {free_slot_for_booking}")
    try:
        result = execute_booking.invoke({"patient_fhir_id": test_patient, "slot_fhir_id": free_slot_for_booking})
        print(json.dumps(result, indent=2))
        # You would normally verify DB state here in a real test suite
    except Exception as e:
        print(f"Error: {e}")

    print(f"\nTesting execute_update for patient {test_patient}, from {booked_slot_by_patient} to {free_slot_for_update}")
    try:
        # Make sure the 'old' slot is actually booked and 'new' slot is free before running
        # This might require setup steps if not guaranteed by previous tests
        print("Pre-update state check recommended (manual or in test setup)")
        result = execute_update.invoke({"patient_fhir_id": test_patient, "old_slot_fhir_id": booked_slot_by_patient, "new_slot_fhir_id": free_slot_for_update})
        print(json.dumps(result, indent=2))
        # Verify DB state after update
    except Exception as e:
        print(f"Error: {e}")

    print(f"\nTesting execute_cancellation for patient {test_patient}, slot {free_slot_for_update}") # Cancel the newly updated slot
    try:
        # Ensure the slot to cancel is actually booked by the patient
        print("Pre-cancel state check recommended (manual or in test setup)")
        result = execute_cancellation.invoke({"patient_fhir_id": test_patient, "slot_fhir_id_to_cancel": free_slot_for_update})
        print(json.dumps(result, indent=2))
        # Verify DB state after cancellation
    except Exception as e:
        print(f"Error: {e}")


    print("\n--- Testing execution_tools.py Complete ---")