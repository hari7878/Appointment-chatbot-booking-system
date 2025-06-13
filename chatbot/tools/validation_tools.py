# tools/validation_tools.py
import logging
import json
import sqlite3
from typing import Dict, Any, List, Optional
from pydantic.v1 import BaseModel, Field # Use v1 Pydantic
from langchain_core.tools import tool
from langchain_core.output_parsers import JsonOutputParser
from langchain_core.prompts import PromptTemplate

# Setup for relative imports and config loading
import sys
import os
# Add the parent directory (chatbot) to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Use relative import for utils within the tools package
try:
    from chatbot.tools.tool_utils import create_db_connection, get_unique_specialties
except ImportError: # Handle running script directly
    from tool_utils import create_db_connection, get_unique_specialties


# Import base LLM
try:
    from llm_config import get_llm
    base_llm = get_llm() # Instantiate the base LLM here
except ImportError:
    print("Warning: tools/validation_tools.py: Could not import get_llm. Validation tool may fail.")
    base_llm = None
except Exception as e:
    print(f"Warning: tools/validation_tools.py: Failed to instantiate LLM: {e}. Validation tool may fail.")
    base_llm = None

logger = logging.getLogger(__name__)
# Ensure logging is configured when run directly or imported
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s [%(filename)s:%(lineno)d] %(message)s')


# --- Pydantic Model for LLM Output (MODIFIED) ---
class MatchedTermInfo(BaseModel):
    """Represents a single matched specialty term."""
    database_term: str = Field(description="The exact specialty name as it appears in the database list.")
    # Optional: Add score if needed, but LLM doesn't easily provide it
    # score: Optional[float] = Field(None, description="Confidence score of the match (if available).")

class ValidatedSpecialty(BaseModel):
    """Structured output for specialty validation."""
    matched_terms_info: Optional[List[MatchedTermInfo]] = Field(description="A list of objects, each containing the exact database specialty term that closely matches the user's term. Should be null or empty if no good match is found.")
    match_found: bool = Field(description="Boolean flag indicating if at least one reasonably close match was found in the database list.")


# --- Tool Input Schema ---
class ValidateSpecialtyInput(BaseModel):
    user_specialty_term: str = Field(description="The potentially misspelled or variant specialty term provided by the user.")

# --- Tool ---
@tool("validate_specialty_term", args_schema=ValidateSpecialtyInput)
def validate_specialty_term(user_specialty_term: str) -> Dict[str, Any]:
    """
    Validates a user-provided specialty term against known specialties using an LLM with structured output.
    Use this *before* searching for doctors if the user provides a specialty term.
    Returns the exact database term(s) if a good match is found, otherwise indicates not found.
    Includes the matched database term explicitly in the output.
    """
    logger.info(f"Tool 'validate_specialty_term' called for user term: '{user_specialty_term}' (JSON Parser Version)")
    if base_llm is None:
        logger.error("Base LLM not initialized. Cannot perform validation.")
        return {"status": "error", "message": "Internal configuration error: Validator LLM not available."}

    conn = None
    try:
        conn = create_db_connection()
        # Get the list of valid, original-case terms from the cache/DB
        valid_db_terms = get_unique_specialties(conn) # Ensures cache is populated
        if not valid_db_terms:
             logger.error("Failed to retrieve valid DB terms for validation.")
             return {"status": "error", "message": "Internal error: Could not retrieve the list of valid specialties."}

        # --- Setup JSON Parser and Prompt ---
        parser = JsonOutputParser(pydantic_object=ValidatedSpecialty)

        prompt_template = """Your task is to match the user's requested medical specialty with a known specialty from a provided list.
User requested specialty: "{user_term}"
List of valid specialties: {valid_list}

Instructions:
1. Find the best match(es) from the 'List of valid specialties' for the 'User requested specialty'. Consider potential typos or variations (e.g., 'cardiolgy' should match 'Cardiologist', 'heart doctor' might match 'Cardiologist').
2. If you find one or more good matches (similarity > ~70%), respond with ONLY a JSON object containing the exact matched term(s) from the list within the structure defined below. 'match_found' must be true.
3. If you find NO good match, respond with ONLY a JSON object where 'match_found' is false and 'matched_terms_info' is null or an empty list.

{format_instructions}

Example (Match Found):
User requested: "cardiolgy"
Valid List: ["Cardiologist", "Neurologist"]
Output JSON:
```json
{{
  "matched_terms_info": [{{ "database_term": "Cardiologist" }}],
  "match_found": true
}}
Example (Multiple Matches Found):
User requested: "Cardiology"
Valid List: ["Cardiologist", "Cardiology Services", "Neurologist"]
Output JSON:
{{
  "matched_terms_info": [{{ "database_term": "Cardiologist" }}, {{ "database_term": "Cardiology Services" }}],
  "match_found": true
}}
Example (No Match Found):
User requested: "NoSuchThing"
Valid List: ["Cardiologist", "Neurologist"]
Output JSON:
{{
  "matched_terms_info": [],
  "match_found": false
}}
Output only the final JSON object. Do not include explanations or preamble.
"""
        prompt = PromptTemplate(
            template=prompt_template,
            input_variables=["user_term", "valid_list"],
            partial_variables={"format_instructions": parser.get_format_instructions()},
        )

        # --- Create Chain and Invoke ---
        chain = prompt | base_llm | parser
        logger.debug(f"Invoking LLM validation chain for term: '{user_specialty_term}'")

        try:
            # Ensure LLM is available before invoking
            if not base_llm: raise RuntimeError("LLM not available for validation.")
            # Run the chain
            # Type hint helps IDE, but parsing might still fail
            validation_result: Dict = chain.invoke({
                "user_term": user_specialty_term,
                "valid_list": json.dumps(valid_db_terms)  # Pass list as JSON string
            })
            logger.info(f"LLM validation parsed result: {validation_result}")

        except Exception as e:
            logger.error(f"LLM validation chain failed for term '{user_specialty_term}': {e}", exc_info=True)
            raw_output = getattr(e, 'llm_output', None) or getattr(e, 'text', None)
            if raw_output: logger.error(f"LLM Raw Output (if available): {raw_output}")
            return {"status": "error",
                    "message": "There was an issue validating the specialty term with the language model (parsing or invocation failed)."}

        # --- Process Parsed Result ---
        # Use .get() for safer access to potentially missing keys
        match_found = validation_result.get("match_found", False)
        matched_info_list = validation_result.get("matched_terms_info", [])

        if match_found and matched_info_list:
            # Extract the database terms and ensure they are valid
            validated_terms = []
            if isinstance(matched_info_list, list):  # Ensure it's a list
                validated_terms = [
                    info["database_term"]
                    for info in matched_info_list
                    # Check if info is a dict, has the key, and the value is in the original list
                    if isinstance(info, dict) and "database_term" in info and info["database_term"] in valid_db_terms
                ]
            else:
                logger.warning(f"LLM returned non-list for matched_terms_info: {matched_info_list}")

            if not validated_terms:
                logger.warning(
                    f"LLM validation chain indicated match_found=True, but terms were invalid or missing: {matched_info_list}")
                return {"status": "not_found", "validated_terms": [],
                        "message": f"Couldn't validate specialty '{user_specialty_term}'. Please try again."}
            else:
                logger.info(f"LLM validation successful via JSON parser. Matched terms: {validated_terms}")
                return {"status": "success", "validated_terms": validated_terms, "matched_info": matched_info_list}
        else:
            # Handle cases where match_found is false or matched_terms_info is empty/null
            logger.warning(
                f"LLM validation via JSON parser indicated no match for '{user_specialty_term}'. Result: {validation_result}")
            return {"status": "not_found", "validated_terms": [],
                    "message": f"I couldn't confidently match '{user_specialty_term}' to a known specialty. Could you please check the spelling or try again?"}

    except sqlite3.Error as e:
        logger.error(f"Database error during specialty validation setup: {e}")
        return {"status": "error", "message": "A database error occurred preparing validation."}
    except Exception as e:
        logger.error(f"Unexpected error during specialty validation: {e}", exc_info=True)
        return {"status": "error", "message": "An unexpected error occurred during validation."}
    finally:
        if conn:
            conn.close()
            logger.debug("Database connection closed.")
if __name__ == "__main__":
    print("--- Testing validation_tools.py with MODIFIED JSON Parser ---")
    # Ensure logging is set up for testing output
    logging.basicConfig(level=logging.DEBUG)
    # Update logger level after basicConfig
    logging.getLogger(__name__).setLevel(logging.DEBUG)
    if base_llm is None:
        print("SKIPPING TESTS: Base LLM is not configured or failed to load.")
    else:
        print(f"Using LLM: {type(base_llm)}")
        # Add more terms if needed
        test_terms = ["Cardiology", "Cardiologist", "cardiolgy", "crdiology", "heart doctor", "Neurology", "Neuro",
                      "General Practice", "Gyn", "NoSuchThing"]
        print("\nTesting validate_specialty_term...")
        # Clear cache for fresh DB fetch in tests if needed
        global _unique_specialties_cache, _specialty_map_cache
        _unique_specialties_cache = None
        _specialty_map_cache = None

        for term in test_terms:
            print(f"\nValidating: '{term}'")
            try:
                # This uses the actual DB and LLM defined in config
                result = validate_specialty_term.invoke({"user_specialty_term": term})
                print(json.dumps(result, indent=2))
                # Add assertions here if running as part of a test suite
                # e.g., if term == "cardiolgy": assert result["status"] == "success" and "Cardiologist" in result["validated_terms"]
            except Exception as e:
                print(f"Error during test for term '{term}': {e}")

    print("\n--- Testing validation_tools.py Complete ---")