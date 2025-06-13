# chatbot/tools/__init__.py

# Import tools from each module to make them available when importing 'tools'
from chatbot.tools.validation_tools import validate_specialty_term
from chatbot.tools.search_tools import (
    find_doctors_and_initial_slots,
    find_more_available_slots,
    find_specific_appointment,
    get_patient_appointments,
)
from chatbot.tools.execution_tools import (
    execute_booking,
    execute_update,
    execute_cancellation,
)

# Define the list of all tools for the agent
available_tools = [
    validate_specialty_term,
    find_doctors_and_initial_slots,
    find_more_available_slots,
    find_specific_appointment,
    get_patient_appointments,
    execute_booking,
    execute_update,
    execute_cancellation,
]

# You can also define __all__ if needed, but the list above is often sufficient
__all__ = [
    "validate_specialty_term",
    "find_doctors_and_initial_slots",
    "find_more_available_slots",
    "find_specific_appointment",
    "get_patient_appointments",
    "execute_booking",
    "execute_update",
    "execute_cancellation",
    "available_tools",
]