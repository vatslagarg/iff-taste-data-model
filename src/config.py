import os

# The root of the project (two levels up from this file: src/config.py -> project root)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DB_PATH = os.path.join(PROJECT_ROOT, "iff_supply_chain.duckdb")

RAW_DATA_DIR = os.path.join(PROJECT_ROOT, "data", "raw")

SCHEMA_RAW = "raw"
SCHEMA_STAGING = "staging"
SCHEMA_INTERMEDIATE = "intermediate"
SCHEMA_MARTS = "marts"

CSV_FILES = {
    "customers": "customers.csv",
    "providers": "providers.csv",
    "raw_materials": "raw_materials.csv",
    "ingredients": "ingredients.csv",
    "flavours": "flavours.csv",
    "recipes": "recipes.csv",
    "sales_transactions": "sales_transactions.csv",
}
