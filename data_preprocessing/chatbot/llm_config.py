# llm_config.py
import logging
from langchain_openai import ChatOpenAI
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_anthropic import ChatAnthropic
from langchain_ollama.chat_models import ChatOllama # Use community for Ollama
from langchain_deepseek import ChatDeepSeek
from langchain_qwq import ChatQwQ
from chatbot.config import (
    LLM_PROVIDER,OPENAI_SETTINGS,GOOGLE_SETTINGS,ANTHROPIC_SETTINGS,OLLAMA_SETTINGS ,DEEPSEEK_SETTINGS,QWEN_SETTINGS
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_llm():
    """
    Initializes and returns the LangChain ChatModel based on configuration.
    """
    logger.info(f"Attempting to load LLM provider: {LLM_PROVIDER}")

    if LLM_PROVIDER == "openai":
        openai_settings_to_use = OPENAI_SETTINGS
        if not openai_settings_to_use["api_key"]:
            raise ValueError("OpenAI API key not found in environment variables.")
        if not openai_settings_to_use["model"]:
            raise ValueError("OpenAI model not selected.")
        # Consider adding model_name selection if needed, e.g., gpt-4o, gpt-3.5-turbo
        return ChatOpenAI(**openai_settings_to_use)

    elif LLM_PROVIDER == "gemini":
        google_settings_to_use = GOOGLE_SETTINGS
        if not google_settings_to_use["google_api_key"]:
            raise ValueError("Google API key not found in environment variables.")
        if not google_settings_to_use["model"]:
            raise ValueError("Google model not selected.")
        # Consider adding model_name selection if needed, e.g., gpt-4o, gpt-3.5-turbo
        return ChatGoogleGenerativeAI(**google_settings_to_use)

    elif LLM_PROVIDER == "anthropic":
        anthropic_settings_to_use = ANTHROPIC_SETTINGS
        if not anthropic_settings_to_use["anthropic_api_key"]:
            raise ValueError("Anthropic API key not found in environment variables.")
        if not anthropic_settings_to_use["model"]:
            raise ValueError("Anthropic model not selected.")
        return ChatAnthropic(**anthropic_settings_to_use)

    elif LLM_PROVIDER == "ollama":
        ollama_settings_to_use = OLLAMA_SETTINGS
        if not ollama_settings_to_use["base_url"]:
            raise ValueError("Ollama no base url set")
        if not ollama_settings_to_use["model"]:
            raise ValueError("Ollama model not set")
        return ChatOllama(**OLLAMA_SETTINGS)

    elif LLM_PROVIDER == "deepseek":
        deepseek_settings_to_use = DEEPSEEK_SETTINGS
        if not deepseek_settings_to_use["api_base"]:
            raise ValueError("Deepseek no base url set")
        if not deepseek_settings_to_use["model"]:
            raise ValueError("Deepseek model not set")
        return ChatDeepSeek(**DEEPSEEK_SETTINGS)

    elif LLM_PROVIDER == "qwen":
        qwen_settings_to_use = QWEN_SETTINGS
        if not qwen_settings_to_use["api_base"]:
            raise ValueError("Qwen no base url set")
        if not qwen_settings_to_use["model"]:
            raise ValueError("Qwen model not set")
        return ChatQwQ(**QWEN_SETTINGS)

    else:
        raise ValueError(f"Unsupported LLM provider: {LLM_PROVIDER}. "
                         "Choose from 'openai', 'gemini', 'anthropic', 'ollama'.")

# --- Basic Test ---
if __name__ == "__main__":
    try:
        llm = get_llm()
        print(f"Successfully loaded LLM: {type(llm)}")
        # Simple invocation test (requires API key to be valid)
        # from langchain_core.messages import HumanMessage
        #response = llm.invoke("hello")
        #print("LLM Response:", response.content)
        #llm = ChatOpenAI(openai_api_base='http://localhost:11434/v1', model='deepseek-r1:8b',api_key="ollama")
        #llm = ChatOllama(base_url='http://localhost:11434', model='deepseek-r1:8b',extract_reasoning=True)
        #llm = ChatOpenAI(openai_api_base='http://localhost:11434/v1', model='deepseek-r1:8b', api_key="ollama")
        #llm=ChatDeepSeek(api_base='http://localhost:11434/v1', model='deepseek-r1:8b', api_key="ollama")
        # llm = ChatQwQ(
        #     model="qwen3:8b",
        #     api_base='http://localhost:11434/v1',
        #     api_key="ollama"
        # )
        response = llm.invoke("hello")
        #print(response.additional_kwargs["reasoning_content"])
        print(response)
        # for chunk in llm.stream("how do you setup gpu on ollama"):
        #     print(chunk.content, end="", flush=True)
    except Exception as e:
        print(f"Error loading LLM: {e}")