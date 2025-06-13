# tools/tool_utils.py
import sqlite3
import logging
from typing import Optional, List, Dict

# Setup for relative imports and config loading
import sys
import os
# Add the parent directory (chatbot) to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
try:
    # Now try importing from chatbot_config assumed to be in the parent directory
    from chatbot.config import DATABASE_PATH
except ImportError:
    # Fallback if running the script directly or config is elsewhere
    # Go up two levels from tools/ to the project root
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    DATABASE_PATH = os.path.join(project_root, "synthea_fhir_data.db") # Adjust as necessary
    print(f"Warning: tools/tool_utils.py: Could not import chatbot_config directly. Using default DB path: {DATABASE_PATH}")
    if not os.path.exists(DATABASE_PATH):
        print("Warning: Default database path does not exist.")


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s [%(filename)s:%(lineno)d] %(message)s')
logger = logging.getLogger(__name__)

def create_db_connection():
    """Creates and returns a connection to the SQLite database."""
    try:
        # Ensure the path exists before connecting
        if not os.path.exists(DATABASE_PATH):
             logger.error(f"Database file not found at path: {DATABASE_PATH}")
             raise sqlite3.OperationalError(f"Database file not found at path: {DATABASE_PATH}")
        conn = sqlite3.connect(DATABASE_PATH)
        conn.execute("PRAGMA foreign_keys = ON;")
        conn.row_factory = sqlite3.Row
        logger.debug("Database connection established.")
        return conn
    except sqlite3.Error as e:
        logger.error(f"Error connecting to database {DATABASE_PATH}: {e}")
        raise

# --- Cache for Unique Specialties ---
_unique_specialties_cache: Optional[List[str]] = None # Stores original casing
_specialty_map_cache: Optional[Dict[str, str]] = None # Stores lowercase -> original

def get_unique_specialties(conn) -> List[str]:
    """Fetches and caches unique original-case specialty/role display names."""
    global _unique_specialties_cache, _specialty_map_cache
    if _unique_specialties_cache is not None and _specialty_map_cache is not None:
        return _unique_specialties_cache

    logger.info("Fetching unique specialties from database...")
    cursor = conn.cursor()
    # Combine non-null distinct values from both columns
    query = """
        SELECT DISTINCT lower(specialty_display) as term, specialty_display as original_term
        FROM practitioner_roles WHERE specialty_display IS NOT NULL AND specialty_display != ''
        UNION
        SELECT DISTINCT lower(role_display) as term, role_display as original_term
        FROM practitioner_roles WHERE role_display IS NOT NULL AND role_display != ''
           AND lower(role_display) NOT IN (SELECT DISTINCT lower(specialty_display) FROM practitioner_roles WHERE specialty_display IS NOT NULL AND specialty_display != '');
    """
    # The UNION logic attempts to prevent duplicate lowercase terms while getting original casing.
    # A simpler dict approach might be better as implemented below.
    try:
        cursor.execute(query)
        results = cursor.fetchall()
        temp_map = {}
        # Build map from lowercase -> original case, prefer specialty if collision
        cursor.execute("SELECT lower(specialty_display) as term, specialty_display as original_term FROM practitioner_roles WHERE specialty_display IS NOT NULL AND specialty_display != ''")
        for row in cursor.fetchall(): temp_map[row['term']] = row['original_term']
        cursor.execute("SELECT lower(role_display) as term, role_display as original_term FROM practitioner_roles WHERE role_display IS NOT NULL AND role_display != ''")
        for row in cursor.fetchall():
            if row['term'] not in temp_map: # Only add role if term not already covered by specialty
                 temp_map[row['term']] = row['original_term']

        _unique_specialties_cache = list(temp_map.values()) # Cache original case terms
        _specialty_map_cache = temp_map # Cache lowercase -> original map
        logger.info(f"Cached {len(_unique_specialties_cache)} unique specialty terms (original case).")
        return _unique_specialties_cache
    except sqlite3.Error as e:
        logger.error(f"Database error fetching unique specialties: {e}")
        return []
    # Connection is managed by the caller tool

def get_specialty_map() -> Dict[str, str]:
    """Returns the lowercase -> original case specialty map (ensure cache is populated)."""
    if _specialty_map_cache is None:
        logger.warning("Specialty map cache accessed before population. Need connection to populate.")
        # Attempting to populate requires a connection, which isn't ideal here.
        # Returning empty dict as a fallback. Ensure get_unique_specialties is called first.
        return {}
    return _specialty_map_cache


# --- Test Block ---
if __name__ == "__main__":
    print("--- Testing tool_utils.py ---")
    logging.basicConfig(level=logging.DEBUG) # Enable debug logging for tests

    conn = None
    try:
        print(f"Attempting connection to DB: {DATABASE_PATH}")
        conn = create_db_connection()
        print("Connection successful.")

        print("\nTesting get_unique_specialties...")
        specialties = get_unique_specialties(conn)
        if specialties:
            print(f"Found {len(specialties)} unique specialties. First few:")
            print(specialties[:10])
            specialty_map = get_specialty_map()
            print("\nLowercase -> Original Map (sample):")
            print(dict(list(specialty_map.items())[:10]))
            # Test cache
            print("\nCalling get_unique_specialties again (should use cache)...")
            specialties_cached = get_unique_specialties(conn)
            print(f"Returned {len(specialties_cached)} specialties from cache.")
            assert len(specialties) == len(specialties_cached) # Basic check
        else:
            print("No specialties found or DB error occurred.")

    except Exception as e:
        print(f"An error occurred during testing: {e}")
    finally:
        if conn:
            conn.close()
            print("\nDatabase connection closed.")
    print("--- Testing tool_utils.py Complete ---")