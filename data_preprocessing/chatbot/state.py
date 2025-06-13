# chatbot/state.py
import operator
from typing import Annotated, Sequence, TypedDict, Optional, List, Dict, Any, Union
from langchain_core.messages import BaseMessage

class AgentState(TypedDict):
    """
    Represents the state of the conversation agent.

    Attributes:
        messages: The history of messages in the conversation.
        patient_fhir_id: The FHIR ID of the patient currently interacting. Set once.
        validated_specialty_terms: List of exact DB specialty terms matched by the validation tool.
        search_results_doctors: Output from find_doctors_and_initial_slots tool.
        search_results_slots: Raw slot data from find_more_available_slots tool.
        appointment_candidates: Details of appointment(s) found by find_specific_appointment.
        new_slot_candidate: Details of a potential new slot for update confirmation.
        clarification_needed: String indicating what specific clarification is needed from the user.
        last_tool_output_status: Status ('success', 'not_found', 'error', etc.) from the last tool call.
    """
    # Core conversation tracking
    messages: Annotated[Sequence[BaseMessage], operator.add]
    patient_fhir_id: Optional[str]

    # Storing search results and context
    validated_specialty_terms: Optional[List[str]] # Result from validate_specialty_term
    search_results_doctors: Optional[List[Dict]]
    search_results_slots: Optional[List[Dict]]
    appointment_candidates: Optional[Union[Dict, List[Dict]]]
    new_slot_candidate: Optional[Dict]

    # Tracking required user actions
    clarification_needed: Optional[str] # e.g., "multiple_appointments_found", "confirm_booking", "confirm_update", "confirm_cancel"

    # Optional: Keep track of last tool output
    last_tool_output_status: Optional[str]

# --- Test Block ---
if __name__ == "__main__":
    print("--- Testing state.py ---")
    # Demonstrate creating an instance (no real test needed here)
    initial_state = AgentState(
        messages=[],
        patient_fhir_id="test-patient-123",
        validated_specialty_terms=None,
        search_results_doctors=None,
        search_results_slots=None,
        appointment_candidates=None,
        new_slot_candidate=None,
        clarification_needed=None,
        last_tool_output_status=None
    )
    print("Created initial AgentState instance:")
    print(initial_state)
    # Example update
    initial_state['validated_specialty_terms'] = ["Cardiologist"]
    print("\nUpdated state:")
    print(initial_state)
    print("--- Testing state.py Complete ---")