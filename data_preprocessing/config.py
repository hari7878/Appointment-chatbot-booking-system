# fhir_processor/config.py
import os
import sqlite3

# --- Configuration ---
# BASE_OUTPUT_DIR =  r"C:\Users\haris\get_synthea_data\synthea\output\fhir"
# DATA_PREPROCESSING_DIR=r"C:\Users\haris\get_synthea_data\synthea\data_preprocessing\output\fhir"
# MAIN_DIR = r"C:\Users\haris\get_synthea_data\synthea"
CONFIG_DIR = os.path.dirname(os.path.abspath(__file__))

# The MAIN_DIR (project root) is one level up from the data_preprocessing directory
MAIN_DIR = os.path.dirname(CONFIG_DIR)

# Now, define all other paths relative to the dynamically found MAIN_DIR
# This makes the script runnable from anywhere, including in Docker.
BASE_OUTPUT_DIR = os.path.join(MAIN_DIR, 'output', 'fhir') # Assumed raw Synthea output
DATA_PREPROCESSING_DIR = os.path.join(MAIN_DIR, 'data_preprocessing', 'output', 'fhir')
# Patterns to identify specific info files
HOSPITAL_FILE_PATTERN = os.path.join(DATA_PREPROCESSING_DIR, "hospitalInformation*.json")
PRACTITIONER_FILE_PATTERN = os.path.join(DATA_PREPROCESSING_DIR, "practitionerInformation*.json")
# Pattern to get all JSON files, will be filtered later
ALL_JSON_PATTERN = os.path.join(DATA_PREPROCESSING_DIR, "*.json")
# Prefixes to identify the info files for exclusion
HOSPITAL_FILE_PREFIX = "hospitalInformation"
PRACTITIONER_FILE_PREFIX = "practitionerInformation"
DATABASE_NAME = os.path.join(MAIN_DIR, "synthea_fhir_data.db")

# --- Mappings for Role Inference ---

# Prioritize more specific terms first
SPECIALTY_KEYWORDS = {
    'cardiology': ['myocardial infarction', 'cardiac', 'heart failure', 'hypertension', 'cholesterol', 'cardiologist'],
    'neurology': ['neurologist', 'stroke', 'headache', 'seizure', 'neurology', 'neuro'],
    'oncology': ['oncology', 'cancer', 'chemotherapy', 'tumor', 'oncologist'],
    'orthopedics': ['orthopedic', 'fracture', 'joint replacement', 'back pain', 'orthopedist'],
    'pediatrics': ['pediatric', 'child', 'infant', 'pediatrician'],
    'pulmonology': ['respiratory', 'asthma', 'copd', 'pneumonia', 'pulmonologist'],
    'endocrinology': ['diabetes', 'thyroid', 'endocrine', 'hormone', 'endocrinologist', 'hyperglycemia'],
    'gastroenterology': ['gastroenterologist', 'digestive', 'colonoscopy', 'gastro'],
    'dermatology': ['dermatologist', 'skin', 'rash', 'derm'],
    'ophthalmology': ['ophthalmologist', 'eye', 'vision'],
    'otorhinolaryngology': ['otorhinolaryngologist', 'ent', 'ear', 'nose', 'throat', 'sinusitis'],
    'psychiatry': ['psychiatrist', 'mental health', 'depression', 'anxiety', 'psych'],
    'urology': ['urologist', 'kidney', 'bladder', 'prostate'],
    'gynecology': ['gynecologist', 'obstetrician', 'pregnancy', 'gyn'],
    'emergency': ['emergency room', 'urgent care', 'emer'],
    'dentistry': ['dental', 'gingivitis', 'periodontist', 'oral surgeon', 'dentist'],
    # Add more specific specialties based on SNOMED codes if needed
    'general practice': ['general examination', 'check up', 'follow-up encounter', 'encounter for problem',
                         'encounter for symptom'],  # Fallback
}
# Map inferred specialty to a potential SNOMED CT or HL7 example role code
# Using SNOMED CT codes where reasonable matches exist from the provided ValueSet example
SNOMED_SYSTEM = 'http://snomed.info/sct'
HL7_ROLE_SYSTEM = 'http://terminology.hl7.org/CodeSystem/practitioner-role'
SPECIALTY_TO_ROLE_CODE = {
    # Specialty: (Code, System, Display)
    'cardiology': ('17561000', SNOMED_SYSTEM, 'Cardiologist'),
    'neurology': ('56397003', SNOMED_SYSTEM, 'Neurologist'),
    'oncology': ('1287641002', SNOMED_SYSTEM, 'Oncologist'),
    'orthopedics': ('22731001', SNOMED_SYSTEM, 'Orthopedic Surgeon'),
    'pediatrics': ('82296001', SNOMED_SYSTEM, 'Pediatrician'),
    'pulmonology': ('41672002', SNOMED_SYSTEM, 'Pulmonologist'),
    'endocrinology': ('61894003', SNOMED_SYSTEM, 'Endocrinologist'),
    'gastroenterology': ('71838004', SNOMED_SYSTEM, 'Gastroenterologist'),
    'dermatology': ('18803008', SNOMED_SYSTEM, 'Dermatologist'),
    'ophthalmology': ('422234006', SNOMED_SYSTEM, 'Ophthalmologist'),
    'otorhinolaryngology': ('61345009', SNOMED_SYSTEM, 'Otorhinolaryngologist'),
    'psychiatry': ('80584001', SNOMED_SYSTEM, 'Psychiatrist'),
    'urology': ('24590004', SNOMED_SYSTEM, 'Urologist'),
    'gynecology': ('83685006', SNOMED_SYSTEM, 'Gynecologist'),
    'emergency': ('309294001', SNOMED_SYSTEM, 'Accident and Emergency Doctor'),
    'dentistry': ('106289002', SNOMED_SYSTEM, 'Dentist'),
    'general practice': ('62247001', SNOMED_SYSTEM, 'Family Medicine Specialist'),
    'doctor': ('doctor', HL7_ROLE_SYSTEM, 'Doctor')  # Default HL7 example
}
DEFAULT_ROLE = ('doctor', HL7_ROLE_SYSTEM, 'Doctor')

# --- Schedule & Slot Generation Configuration ---
SCHEDULE_HORIZON_DAYS = 28  # How many days into the future the schedule planning horizon covers
SLOT_WORKING_DAY_START_HOUR = 9  # 9 AM
SLOT_WORKING_DAY_END_HOUR = 17  # 5 PM (slots must end by this time)
SLOT_DURATIONS_MINUTES = [15, 30, 45, 60]  # Possible slot lengths
SLOT_STATUS_CHOICES = ['free', 'busy']
SLOT_STATUS_WEIGHTS = [3, 1]  # Ratio: 3 'free' for every 1 'busy'

# --- Database Schema ---
TABLE_DEFINITIONS = {
    "patients": """
    CREATE TABLE IF NOT EXISTS patients (
        patient_fhir_id TEXT PRIMARY KEY,
        first_name TEXT,
        middle_name TEXT,
        last_name TEXT,
        prefix TEXT,
        mothers_maiden_name TEXT,
        dob DATE,
        gender TEXT,
        marital_status TEXT,
        ssn TEXT,
        drivers_license TEXT,
        passport TEXT,
        mrn TEXT,
        mrn_system TEXT,
        phone_home TEXT,
        address_line TEXT,
        address_city TEXT,
        address_state TEXT,
        address_postal_code TEXT,
        address_country TEXT,
        birth_city TEXT,
        birth_state TEXT,
        birth_country TEXT,
        language TEXT
    );
    """,
    "hospitals": """
    CREATE TABLE IF NOT EXISTS hospitals (
        hospital_fhir_id TEXT PRIMARY KEY,
        synthea_identifier TEXT UNIQUE, -- Keep for potential cross-reference
        name TEXT,
        phone TEXT,
        address_line TEXT,
        address_city TEXT,
        address_state TEXT,
        address_postal_code TEXT,
        address_country TEXT
    );
    """,
    "practitioners": """
    CREATE TABLE IF NOT EXISTS practitioners (
        practitioner_npi TEXT PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        prefix TEXT,
        email TEXT,
        gender TEXT,
        address_line TEXT,
        address_city TEXT,
        address_state TEXT,
        address_postal_code TEXT,
        address_country TEXT
    );
    """,
    "schedules": """
    CREATE TABLE IF NOT EXISTS schedules (
        schedule_fhir_id TEXT PRIMARY KEY,
        practitioner_npi TEXT NOT NULL,
        active INTEGER DEFAULT 1, -- Simulating boolean
        planning_horizon_start DATETIME NOT NULL,
        planning_horizon_end DATETIME NOT NULL,
        comment TEXT,
        FOREIGN KEY (practitioner_npi) REFERENCES practitioners (practitioner_npi),
        UNIQUE (practitioner_npi) -- Assuming one primary schedule per practitioner in this model
    );
    """,
    "slots": """
    CREATE TABLE IF NOT EXISTS slots (
        slot_fhir_id TEXT PRIMARY KEY,
        schedule_fhir_id TEXT NOT NULL,
        status TEXT NOT NULL CHECK(status IN ('free', 'busy')), -- Constrain status values
        start_time DATETIME NOT NULL,
        end_time DATETIME NOT NULL,
        comment TEXT,
        FOREIGN KEY (schedule_fhir_id) REFERENCES schedules (schedule_fhir_id),
        UNIQUE (schedule_fhir_id, start_time) -- Prevent slots starting at the exact same time for one schedule
    );
    """,
    "practitioner_roles": """
    CREATE TABLE IF NOT EXISTS practitioner_roles (
        role_id INTEGER PRIMARY KEY AUTOINCREMENT,
        practitioner_npi TEXT NOT NULL,
        hospital_fhir_id TEXT, -- Associated Organization FHIR ID (can be NULL)
        role_code TEXT,
        role_system TEXT,
        role_display TEXT,
        specialty_code TEXT, -- Often same as role_code for SNOMED based roles
        specialty_system TEXT,
        specialty_display TEXT, -- Often same as role_display
        FOREIGN KEY (practitioner_npi) REFERENCES practitioners (practitioner_npi),
        FOREIGN KEY (hospital_fhir_id) REFERENCES hospitals (hospital_fhir_id),
        UNIQUE(practitioner_npi, hospital_fhir_id, role_code, specialty_code) -- Prevent exact duplicates
    );
    """,
    "encounters": """
    CREATE TABLE IF NOT EXISTS encounters (
        encounter_id TEXT PRIMARY KEY,
        patient_fhir_id TEXT NOT NULL,
        practitioner_npi TEXT,
        hospital_fhir_id TEXT, -- Store the actual FHIR ID of the hospital
        start_time DATETIME,
        end_time DATETIME,
        encounter_class_code TEXT,
        encounter_type_code TEXT,
        encounter_type_system TEXT,
        encounter_type_display TEXT,
        FOREIGN KEY (patient_fhir_id) REFERENCES patients (patient_fhir_id),
        FOREIGN KEY (practitioner_npi) REFERENCES practitioners (practitioner_npi),
        FOREIGN KEY (hospital_fhir_id) REFERENCES hospitals (hospital_fhir_id)
    );
    """,
    "appointments": """
CREATE TABLE IF NOT EXISTS appointments (
    appointment_id INTEGER PRIMARY KEY AUTOINCREMENT,
    patient_fhir_id TEXT NOT NULL,
    slot_fhir_id TEXT NOT NULL UNIQUE, -- Link to the specific booked slot, ensure slot only booked once
    booking_time DATETIME DEFAULT CURRENT_TIMESTAMP,
    status TEXT DEFAULT 'confirmed', -- e.g., 'confirmed', 'cancelled' (might not need if deleting)
    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (patient_fhir_id) REFERENCES patients (patient_fhir_id),
    FOREIGN KEY (slot_fhir_id) REFERENCES slots (slot_fhir_id)
        ON DELETE CASCADE -- Optional: If a slot is somehow deleted, remove associated appointments
);
""",
    "appointment_patient_idx": """
    CREATE INDEX IF NOT EXISTS appointment_patient_idx ON appointments (patient_fhir_id);
    """,
    "appointment_slot_idx": """
    CREATE INDEX IF NOT EXISTS appointment_slot_idx ON appointments (slot_fhir_id);
    """
}


# Helper to format datetime for SQLite compatibility (ISO 8601)
def format_datetime_for_db(dt_obj):
    if dt_obj:
        return dt_obj.strftime('%Y-%m-%dT%H:%M:%SZ')
    return None
