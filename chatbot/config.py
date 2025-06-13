# chatbot_config.py

import os
from dotenv import load_dotenv

load_dotenv() # Load environment variables from .env file


# --- LLM Configuration ---
# Options: "openai", "gemini", "anthropic", "ollama"
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "deepseek").lower()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_SETTINGS = {
    # REQUIRED: User MUST typically specify the model
    "model": "deepseek-r1:8b",
    "openai_api_base":'http://localhost:11434/v1',
    # --- User Overrides/Additions ---(REFER DOCUMENTATION for ChatOpenAI)
    #"temperature": 0.5,         # Override the default temperature
    # "max_tokens": 2000,       # Example: Override default max_tokens
    "api_key": OPENAI_API_KEY,         # Example: Explicitly set API key (overrides ENV, use with caution)
    #"frequency_penalty": 0.2, # Add a parameter not in the defaults
    # organization: "org-...", # Example: Specify organization
}
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
GOOGLE_SETTINGS = {
    # REQUIRED: User MUST typically specify the model
    "model": "gemini-2.0-flash",

    # --- User Overrides/Additions ---(REFER DOCUMENTATION for ChatGoogleGenerativeAI)
    # "temperature": 0.5,         # Override the default temperature
    # "max_output_tokens": 2000,       # Example: Override default max_tokens
    "google_api_key": GOOGLE_API_KEY,  # Example: Explicitly set API key (overrides ENV, use with caution)
}
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
ANTHROPIC_SETTINGS= {
    # REQUIRED: User MUST typically specify the model
    "model": "claude-3-sonnet-20240229",

    # --- User Overrides/Additions ---(REFER DOCUMENTATION for ChatAnthropic)
    # "temperature": 0.5,         # Override the default temperature
    # "max_tokens": 2000,       # Example: Override default max_tokens
    "anthropic_api_key": ANTHROPIC_API_KEY,  # Example: Explicitly set API key (overrides ENV, use with caution)
}
# For Ollama, specify the base URL and model name
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3") # Or mistral, etc,should directly correspond to the name (and optionally tag) of the model that you have downloaded
OLLAMA_SETTINGS={
    "base_url":OLLAMA_BASE_URL,
    "model":OLLAMA_MODEL,
    # --- User Overrides/Additions ---(REFER DOCUMENTATION for ChatOllama)
    # "num_gpu": 2,       # Example: Override default None
}
DEEPSEEK_BASE_URL = os.getenv("DEEPSEEK_BASE_URL", "http://localhost:11434")
DEEPSEEK_API_KEY=os.getenv("DEEPSEEK_API_KEY", "ollama")
DEEPSEEK_MODEL = os.getenv("DEEPSEEK_MODEL", 'deepseek-r1:8b')
DEEPSEEK_SETTINGS={
    "api_base":DEEPSEEK_BASE_URL,
    "model":DEEPSEEK_MODEL,
    "api_key":DEEPSEEK_API_KEY
    # --- User Overrides/Additions ---(REFER DOCUMENTATION for ChatOllama)
    # "num_gpu": 2,       # Example: Override default None
}

QWEN_BASE_URL = os.getenv("QWEN_BASE_URL", "http://localhost:11434")
QWEN_API_KEY=os.getenv("QWEN_API_KEY", "ollama")
QWEN_MODEL = os.getenv("QWEN_MODEL", 'qwen3:8b')
QWEN_SETTINGS={
    "api_base":QWEN_BASE_URL,
    "model":QWEN_MODEL,
    "api_key":QWEN_API_KEY,

    # --- User Overrides/Additions ---(REFER DOCUMENTATION for ChatOllama)
    # "num_gpu": 2,       # Example: Override default None
}
# --- Database Configuration ---
# Assumes the DB is in the parent directory relative to the chatbot scripts
# current_dir = r"C:\Users\haris\get_synthea_data\synthea"
# # Adjust this path if your directory structure is different
# DATABASE_PATH = os.path.join(current_dir, "synthea_fhir_data.db")

CHATBOT_DIR = os.path.dirname(os.path.abspath(__file__))
# Get the project root directory by going up one level from the chatbot directory
PROJECT_ROOT = os.path.dirname(CHATBOT_DIR)
# Construct the full, portable path to the database
DATABASE_PATH = os.path.join(PROJECT_ROOT, "synthea_fhir_data.db")

# --- Chatbot Settings ---
DEFAULT_PATIENT_ID = "1f497115-11b3-6ee8-d508-9360e220db37" # Placeholder: In a real app, you'd get this via login/selection
MAX_SLOT_SUGGESTIONS_PER_DAY = 3
SUGGESTION_WINDOW_DAYS = 7 # Suggest slots for the next 7 days

