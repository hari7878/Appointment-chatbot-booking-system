# app.py
import streamlit as st
from uuid import uuid4
import json
import logging

# --- Core Chatbot Imports ---
# This assumes app.py is in the project root, so 'chatbot' is a package.
from chatbot.graph import graph  # The compiled LangGraph object
from chatbot.config import DEFAULT_PATIENT_ID, LLM_PROVIDER
from langchain_core.messages import HumanMessage, AIMessage

# --- UI Configuration ---
st.set_page_config(
    page_title="HealthSched Bot",
    page_icon="🏥",
    layout="wide"
)

st.title("🏥 HealthSched: Your AI Appointment Assistant")
st.caption(f"Powered by LangGraph and {LLM_PROVIDER.capitalize()}")

# --- Logger for UI ---
ui_logger = logging.getLogger(__name__)

# --- Session State Initialization ---
# This is crucial for maintaining the conversation state across Streamlit reruns.
if "thread_id" not in st.session_state:
    # Each new browser session gets a unique thread_id for conversation memory
    st.session_state.thread_id = str(uuid4())
    ui_logger.info(f"New session started. Thread ID: {st.session_state.thread_id}")

if "messages" not in st.session_state:
    # Initialize with a greeting from the AI
    st.session_state.messages = [
        AIMessage(content="Hello! I'm HealthSched, your medical appointment assistant. How can I help you today? You can ask me to find a doctor, book, view, or cancel appointments.")
    ]

# --- Sidebar for Controls and Info ---
with st.sidebar:
    st.header("Chat Controls")
    if st.button("Start New Chat", type="primary"):
        st.session_state.messages = [
            AIMessage(content="Hello again! Let's start over. How can I help you?")
        ]
        st.session_state.thread_id = str(uuid4())
        st.success("New chat session started!")
        st.rerun()

    st.divider()
    st.header("Session Information")
    st.info(f"Operating for Patient ID: **{DEFAULT_PATIENT_ID}**")
    st.warning(f"Session Thread ID: `{st.session_state.thread_id}`")
    st.caption("This ID links your conversation turns together. A new one is created for each new chat.")

# --- Main Chat Interface ---

# 1. Display existing messages from history
for message in st.session_state.messages:
    role = "user" if isinstance(message, HumanMessage) else "assistant"
    with st.chat_message(role):
        st.markdown(message.content)

# 2. Get new user input
if prompt := st.chat_input("Ask about appointments..."):
    # Add user message to session state and display it
    st.session_state.messages.append(HumanMessage(content=prompt))
    with st.chat_message("user"):
        st.markdown(prompt)

    # Prepare for AI response
    with st.chat_message("assistant"):
        with st.spinner("Thinking... (The agent is processing your request)"):
            try:
                # Prepare the input for the LangGraph. It expects a list of messages.
                # The checkpointer will automatically load the full history for this thread_id.
                inputs = {"messages": [HumanMessage(content=prompt)]}

                # Get the thread_id from session state for the config
                config = {"configurable": {"thread_id": st.session_state.thread_id}}

                # Invoke the graph. This is where all the magic happens!
                # The `graph` object handles the LLM calls, tool execution, and state updates.
                final_state = graph.invoke(inputs, config=config)

                # The final AI response is the last message in the state
                ai_response = final_state['messages'][-1]
                response_content = ai_response.content

                # Display the AI's response
                st.markdown(response_content)

                # Add the AI's response to the message history for future turns
                st.session_state.messages.append(AIMessage(content=response_content))

                # (Optional) Display the final state for debugging
                with st.expander("Show Agent's Final State for This Turn"):
                    # Convert BaseMessage objects to a serializable format for st.json
                    state_for_display = final_state.copy()
                    state_for_display['messages'] = [msg.to_json() for msg in state_for_display['messages']]
                    st.json(state_for_display)

            except Exception as e:
                ui_logger.error(f"Error during graph invocation: {e}", exc_info=True)
                error_message = f"Sorry, an error occurred: {e}"
                st.error(error_message)
                st.session_state.messages.append(AIMessage(content=error_message))