# chatbot/graph.py
import json
import logging
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.sqlite import SqliteSaver
from langchain_core.messages import HumanMessage, AIMessage, ToolMessage, BaseMessage,SystemMessage
from langchain_core.runnables import RunnableConfig
from typing import List

# Setup for relative imports and config loading
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '.'))) # Add chatbot dir to path

# Import revised state and tools
from chatbot.state import AgentState
from chatbot.llm_config import get_llm
# Import the combined list from the tools package __init__
from chatbot.tools import available_tools
from chatbot.prompts import SYSTEM_PROMPT
from chatbot.config import DEFAULT_PATIENT_ID

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s [%(filename)s:%(lineno)d] %(message)s')
logger = logging.getLogger(__name__)

# Initialize LLM and bind tools
try:
    llm = get_llm()
    # Create tool schema descriptions for the prompt
    tool_schemas = "\n".join(
        [f"- {tool.name}: {tool.description}\n  Input Schema: {tool.args_schema.schema()['properties']}"
         for tool in available_tools]
    )
    llm_with_tools = llm.bind_tools(available_tools)
except Exception as e:
    logger.error(f"Failed to initialize LLM or bind tools: {e}", exc_info=True)
    exit(1) # Exit if LLM setup fails

# --- Graph Nodes ---

def agent_node(state: AgentState, config: RunnableConfig):
    """Agent node: Decides action based on state, calls LLM."""
    logger.debug(f"--- Agent Node --- Current State:\n{state}")

    current_patient_id = state.get('patient_fhir_id') or DEFAULT_PATIENT_ID
    formatted_prompt = SYSTEM_PROMPT.format(
        tool_schemas=tool_schemas,
        patient_fhir_id=current_patient_id
    )

    # Prepare message list for LLM
    messages: List[BaseMessage] = [SystemMessage(content=formatted_prompt)] # Start with system prompt
    messages.extend(state['messages']) # Add history

    # --- Inject State Summary (Optional but helpful) ---
    state_summary_parts = []
    if state.get('clarification_needed'):
        state_summary_parts.append(f"Context: System is waiting for user clarification ({state.get('clarification_needed')}).")
        if state.get('appointment_candidates'):
             # Summarize candidates briefly
             candidates = state['appointment_candidates']
             summary = f"Found candidate appointment(s): {json.dumps(candidates)[:200]}..." # Limit length
             state_summary_parts.append(summary)
    elif state.get('validated_specialty_terms'):
         # If validation just happened, mention it so agent knows to search
         state_summary_parts.append(f"Context: Specialty validation successful for terms: {state['validated_specialty_terms']}. Next step is likely finding doctors.")
    # Add more context summaries as needed

    if state_summary_parts:
         summary_text = "\n".join(state_summary_parts)
         # Add summary as an AI message right before the last Human message for context
         last_human_idx = -1
         for i in range(len(messages) - 1, 0, -1): # Start from end, ignore sys prompt at index 0
             if isinstance(messages[i], HumanMessage):
                 last_human_idx = i
                 break
         summary_message = AIMessage(content=f"--- Current Context ---\n{summary_text}\n--- End Context ---")
         if last_human_idx != -1:
              messages.insert(last_human_idx, summary_message)
         else: # If no human message (e.g., first turn after initial greeting), just append
              messages.append(summary_message)
    # --- End State Summary Injection ---


    logger.info("Invoking LLM with tools...")
    # logger.debug(f"Messages sent to LLM (showing first 1000 chars):\n{str(messages)[:1000]}\n...") # Can be very verbose
    #try:
    response = llm_with_tools.invoke(messages, config=config)
    logger.info(f"LLM Response Content: {response.content}")
    logger.info(f"LLM Response Tool Calls: {response.tool_calls}")
    # except Exception as e:
    #     logger.error(f"LLM invocation failed: {e}", exc_info=True)
    #     # Return a generic error message to the state
    #     response = AIMessage(content="Sorry, I encountered an internal error trying to process your request.")

    # Return the AIMessage, potentially containing tool calls
    # State updates happen *after* tool execution in the tool_node
    return {"messages": [response]}


def tool_node(state: AgentState):
    """Tool node: Executes tools and updates state based on results."""
    logger.debug(f"--- Tool Node ---")
    last_message = state['messages'][-1]
    if not isinstance(last_message, AIMessage) or not last_message.tool_calls:
        logger.debug("No tool call found in last AI message.")
        return {} # No state changes if no tool called

    tool_outputs = []
    last_tool_output_dict = {} # Store the raw output of the last tool call
    tool_status = "unknown"

    # --- State updates - Reset potentially stale fields before processing results ---
    # Keep validated_specialty_terms until a search is done or flow resets
    new_state_updates = {
        "clarification_needed": None,
        "appointment_candidates": None,
        "new_slot_candidate": None,
        "search_results_doctors": None,
        "search_results_slots": None,
        # Don't reset validated_specialty_terms here unless explicitly needed
    }

    for tool_call in last_message.tool_calls:
        tool_name = tool_call['name']
        tool_args = tool_call['args']
        logger.info(f"Executing tool '{tool_name}' with args: {tool_args}")

        selected_tool = next((t for t in available_tools if t.name == tool_name), None)
        output_content = ""
        output_dict = {}

        if not selected_tool:
            output_content = f"Error: Tool '{tool_name}' not found."
            tool_status = "error"
            output_dict = {"status": tool_status, "message": output_content}
            logger.error(output_content)
        else:
            #try:
                # Inject patient_fhir_id if required by the tool and not provided by LLM
            if 'patient_fhir_id' in selected_tool.args_schema.__fields__ and 'patient_fhir_id' not in tool_args:
                 current_patient_id = state.get('patient_fhir_id') or DEFAULT_PATIENT_ID
                 if current_patient_id:
                      logger.warning(f"Tool '{tool_name}' called without patient_fhir_id. Injecting ID: {current_patient_id}")
                      tool_args['patient_fhir_id'] = current_patient_id
                 else:
                      logger.error(f"Tool '{tool_name}' requires patient_fhir_id, but none available in state or default.")
                      raise ValueError(f"Missing patient_fhir_id for tool {tool_name}")

            output_dict = selected_tool.invoke(tool_args) # Tool returns dict
            output_content = json.dumps(output_dict) # Convert dict to string for ToolMessage
            tool_status = output_dict.get("status", "unknown")
            logger.info(f"Tool '{tool_name}' raw output dict: {output_dict}")

            # --- State updates based on TOOL RESULT ---
            if tool_name == "validate_specialty_term":
                if tool_status == 'success':
                    # Store validated terms for the agent node to use next
                    new_state_updates["validated_specialty_terms"] = output_dict.get("validated_terms")
                else:
                    # Validation failed, clear any potentially stored terms
                    new_state_updates["validated_specialty_terms"] = None

            elif tool_name == "find_doctors_and_initial_slots":
                 # Clear validated terms after use? Optional, depends on desired flow.
                 # new_state_updates["validated_specialty_terms"] = None
                 if tool_status == 'success':
                      new_state_updates["search_results_doctors"] = output_dict.get("doctors")

            elif tool_name == "find_more_available_slots" and tool_status == 'success':
                new_state_updates["search_results_slots"] = output_dict.get("raw_slots") # Store raw data

            elif tool_name == "find_specific_appointment":
                # Clear previous search results when looking for specific appt
                new_state_updates["search_results_doctors"] = None
                new_state_updates["search_results_slots"] = None
                if tool_status == 'found_specific':
                    new_state_updates["appointment_candidates"] = output_dict.get("appointment_details")
                    new_state_updates["clarification_needed"] = "confirm_action" # Agent needs to ask Y/N
                elif tool_status == 'found_multiple':
                    new_state_updates["appointment_candidates"] = output_dict.get("raw_appointments")
                    new_state_updates["clarification_needed"] = "multiple_appointments_found" # Agent needs to ask for ID

            # Clear candidates and clarification if an execution succeeds
            elif tool_name.startswith("execute_") and tool_status == 'success':
                 new_state_updates["appointment_candidates"] = None
                 new_state_updates["new_slot_candidate"] = None
                 new_state_updates["clarification_needed"] = None
                 # Maybe clear search results too after successful action?
                 new_state_updates["search_results_doctors"] = None
                 new_state_updates["search_results_slots"] = None
                 new_state_updates["validated_specialty_terms"] = None


            # except Exception as e:
            #     logger.error(f"Error executing tool '{tool_name}': {e}", exc_info=True)
            #     output_content = f"Error executing tool {tool_name}: {str(e)}"
            #     tool_status = "error"
            #     output_dict = {"status": tool_status, "message": output_content}

        tool_outputs.append(ToolMessage(content=output_content, tool_call_id=tool_call['id']))
        last_tool_output_dict = output_dict # Store the dict output

    # Update state dictionary with messages and specific fields
    new_state_updates["messages"] = tool_outputs
    new_state_updates["last_tool_output_status"] = tool_status
    # Note: last_tool_output_dict is NOT added to state by default, but could be if needed

    logger.debug(f"Returning updates from tool node: {new_state_updates}")
    return new_state_updates

# --- Graph Definition ---
workflow = StateGraph(AgentState)

# Add nodes
workflow.add_node("agent", agent_node)
workflow.add_node("tools", tool_node)

# Define edges
workflow.set_entry_point("agent")

def should_continue(state: AgentState) -> str:
    """Decide next step: call tools or end."""
    last_message = state['messages'][-1]
    # If the last message is an AIMessage with tool calls, route to tools
    if isinstance(last_message, AIMessage) and last_message.tool_calls:
        logger.debug("Decision: Call Tools")
        return "tools"
    # Otherwise, the cycle ends and control returns to the user
    else:
        logger.debug("Decision: End Cycle")
        return END

# Add conditional edge from agent
workflow.add_conditional_edges(
    "agent",
    should_continue,
    {
        "tools": "tools", # If tools are called, go to tools node
        END: END          # Otherwise, end the graph iteration
    }
)

# Add edge from tools back to agent
workflow.add_edge("tools", "agent") # Always return to agent after tools run

# --- Compile the Graph ---
# Use SqliteSaver for persistence. Creates 'chatbot_memory.sqlite' if it doesn't exist.
# memory = SqliteSaver.from_conn_string(":memory:") # In-memory for simple testing
memory_path = "chatbot_memory.sqlite"
logger.info(f"Using SQLite checkpointer at: {memory_path}")
#memory = SqliteSaver.from_conn_string(memory_path)
import sqlite3
conn = sqlite3.connect("chatbot_memory.sqlite", check_same_thread=False)
memory = SqliteSaver(conn)
# Compile the graph with checkpointing
graph = workflow.compile(checkpointer=memory)
logger.info("LangGraph compiled successfully.")

# --- Test Block ---
if __name__ == "__main__":
    print("\n--- Testing Graph Execution (LLM Validation Flow) ---")
    # Use a unique thread ID for each test run if using persistent memory
    # thread_id = f"test-thread-{datetime.now().strftime('%Y%m%d%H%M%S')}"
    thread_id = "test-thread-llm-validation-2" # Fixed ID for repeatable testing (clear DB file if needed)
    thread_config = {"configurable": {"thread_id": thread_id}}
    patient_id = DEFAULT_PATIENT_ID

    print(f"Using Thread ID: {thread_id}")
    print(f"Using Patient ID: {patient_id}")

    # Function to run an interaction cycle
    def run_interaction(user_message_content, current_config):
        print(f"\nYou: {user_message_content}")
        inputs = {"messages": [HumanMessage(content=user_message_content)]}
        # The checkpointer handles loading/saving state based on thread_id
        # No need to manually inject patient_id after the first turn (handled by state/prompt)
        #try:
            # Stream events for more visibility (optional)
            # for event in graph.stream(inputs, config=current_config):
            #     print(f"Event: {event}")
            #     print("---")
            # final_state = graph.get_state(current_config) # Get state after stream

            # Or just invoke
        final_state = graph.invoke(inputs, config=current_config)

        if final_state and final_state.get('messages'):
             ai_response = final_state['messages'][-1]
             print(f"AI: {ai_response.content}")
             # print(f"DEBUG State:\n{json.dumps(final_state, indent=2, default=str)}") # Print state for debugging
             return final_state
        else:
             print("AI: (No final message found in state)")
             return None
        # except Exception as e:
        #     print(f"AI: Error during interaction: {e}")
        #     logger.error(f"Graph invocation error", exc_info=True)
        #     return None

    # --- Example Conversation Flow ---
    print("\n--- Starting Conversation ---")
    # 1. Initial request - needs specialty
    current_state = run_interaction("Hi, I need to book an appointment.", thread_config)

    # 2. Provide specialty - should trigger validation, then search
    if current_state:
        current_state = run_interaction("I need a heart doctor, maybe cardiology?", thread_config) # Test synonym/variation

    # 3. Provide misspelled specialty - should trigger validation (fail/clarify), then potentially ask again
    if current_state:
         current_state = run_interaction("How about for crdiology?", thread_config) # Test typo

    # 4. Assume user clarifies with valid term and agent presents doctors/slots
    #    (Manual step here - assume previous step resulted in doctor list)
    print("\n(Simulating user sees doctor list and chooses slot...)")
    test_slot_id_to_book = " slot-sch-999997-582e4abd05fc" # REPLACE with a valid FREE slot ID from previous output/DB
    if current_state:
        current_state = run_interaction(f"Great, please book Slot ID {test_slot_id_to_book} for me.", thread_config)

    # 5. View appointments
    if current_state:
        current_state = run_interaction("Show me my appointments.", thread_config)

    # 6. Initiate cancellation - needs clarification potentially
    if current_state:
        current_state = run_interaction("I need to cancel my appointment for tomorrow morning.", thread_config)

    # 7. User confirms cancellation with Slot ID (use the one booked above)
    if current_state and test_slot_id_to_book:
         # Check if clarification was requested in previous turn (optional check)
         if current_state.get("clarification_needed"):
              print("\n(Agent likely asked for clarification, user provides ID)")
              current_state = run_interaction(f"Yes, please cancel Slot ID {test_slot_id_to_book}.", thread_config)
         else: # Agent might have found it directly if info was specific enough
              print(f"\n(Agent might have found appointment directly or failed, attempting cancel command anyway for {test_slot_id_to_book})")
              # Note: The 'execute_cancellation' tool should only be called by the agent
              # after it has confirmed the specific appointment with the user.
              # This direct call might bypass the intended confirmation logic.
              # Better test: "Yes, confirm cancellation for Slot ID XYZ" if agent asked.
              current_state = run_interaction(f"Confirm cancellation for {test_slot_id_to_book}", thread_config)



    print("\n--- Graph Test Complete ---")