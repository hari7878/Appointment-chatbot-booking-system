# HealthSched AI: A Medical Appointment Chatbot


[![HealthSched AI Full Demo Video]](https://www.youtube.com/watch?v=u0QpZdaBxzI&ab_channel=Hari)
HealthSched AI is a  conversational AI agent designed to streamline the medical appointment scheduling process. Built on a foundation of synthetic FHIR data, this project features a robust data processing pipeline and a powerful LangGraph-based agent capable of understanding user requests, searching for providers, checking availability, and managing appointments.

The application is designed to be run locally and supports multiple LLM providers, with a dynamic configuration UI that allows users to bring their own models and API keys.



## Features

-   **End-to-End Data Pipeline:** Processes raw Synthea FHIR data (JSON) into a structured, queryable SQLite database.
-   **Stateful Conversational Agent:** Built with LangGraph to manage complex, multi-turn conversations and remember context.
-   **Dynamic Tool Use:** The agent intelligently decides which tools to use for tasks like:
    -   Validating medical specialty terms (e.g., "cardiolgy" -> "Cardiologist").
    -   Finding doctors and their availability.
    -   Booking, viewing, updating, and canceling appointments.
-   **Multi-LLM Support:** Easily switch between local models (via Ollama) and cloud-based APIs (OpenAI, Google Gemini, Anthropic).
-   **Dynamic Configuration UI:** A user-friendly Streamlit interface allows users to provide their own LLM credentials and model settings for their session.




## Local Setup and Installation

Follow these steps to get the entire application running on your local machine.

### Prerequisites


-   [Python 3.10+](https://www.python.org/)
-   [Ollama](https://ollama.com/) (if you plan to use local models). Make sure the Ollama desktop application is installed and running.

### 1. Clone the Repository

```bash
git clone https://github.com/hari7878/Appointment-chatbot-booking-system.git
```

### 2. Set Up the Python Environment
```bash
python -m venv venv
source venv/bin/activate
venv\Scripts\activate
pip install -r requirements.txt
```
### 3. Configuration
Refer the chatbot/.env file and chatbot/config.py file and add secrets.Model parameters follow Langchain documentation for creating model instances from the chatbot/config.py (Refer chatbot/llm_config.py)

### 4. Create the Database
The chatbot relies on a pre-populated SQLite database. Run the data processing pipeline to create it from your FHIR data files. This step only needs to be done once.
Note: Before running, ensure your Synthea JSON files are placed in the correct directory as expected by the processing scripts (data_preprocessing/output/fhir directory).
```bash
python -m data_preprocessing.main_processor
```

### 5.Running the Application
With the setup complete, you can now launch the chatbot.
Make sure your Python virtual environment is activated.
Make sure the Ollama application is running (if using a local model).
From the project's root directory, run the Streamlit application:
```bash
streamlit run app.py
```
