from src.config import SCHEMA_RAW, SCHEMA_STAGING
from src.utils import get_connection, create_schema_if_not_exists, print_table_info, get_parse_date_sql


def create_staging_tables():

    print("STEP 2: Creating staging tables (clean & standardize)")

    con = get_connection()
    create_schema_if_not_exists(con, SCHEMA_STAGING)

    #stg_customers

    con.execute(f"""
        CREATE OR REPLACE TABLE {SCHEMA_STAGING}.stg_customers AS
        SELECT
            customer_id::INTEGER AS customer_id,
            TRIM(name) AS customer_name,
            TRIM(location_city) AS customer_city,
            TRIM(location_country) AS customer_country,
            generation_date,
            batch_number::INTEGER AS batch_number
        FROM {SCHEMA_RAW}.customers
    """)
    print_table_info(con, SCHEMA_STAGING, "stg_customers")

    #stg_providers
    
    con.execute(f"""
        CREATE OR REPLACE TABLE {SCHEMA_STAGING}.stg_providers AS
        SELECT
            provider_id::INTEGER AS provider_id,
            TRIM(name) AS provider_name,
            TRIM(location_city) AS provider_city,
            TRIM(location_country) AS provider_country,
            generation_date,
            batch_number::INTEGER AS batch_number
        FROM {SCHEMA_RAW}.providers
    """)
    print_table_info(con, SCHEMA_STAGING, "stg_providers")

    #stg_raw_materials

    con.execute(f"""
        CREATE OR REPLACE TABLE {SCHEMA_STAGING}.stg_raw_materials AS
        SELECT
            raw_material_id::INTEGER AS raw_material_id,
            TRIM(name) AS raw_material_name,
            generation_date,
            batch_number::INTEGER AS batch_number
        FROM {SCHEMA_RAW}.raw_materials
    """)
    print_table_info(con, SCHEMA_STAGING, "stg_raw_materials")

    #stg_ingredients
    parse_date = get_parse_date_sql("generation_date")
    con.execute(f"""
        CREATE OR REPLACE TABLE {SCHEMA_STAGING}.stg_ingredients AS
        SELECT
            ingredient_id::INTEGER AS ingredient_id,
            TRIM(name) AS ingredient_name,
            TRIM(chemical_formula) AS chemical_formula,
            weight_in_grams,
            cost_per_gram,
            provider_id::INTEGER AS provider_id,
            {parse_date} AS generation_date,
            batch_number::INTEGER AS batch_number
        FROM {SCHEMA_RAW}.ingredients
    """)
    print_table_info(con, SCHEMA_STAGING, "stg_ingredients")

    #stg_flavours

    con.execute(f"""
        CREATE OR REPLACE TABLE {SCHEMA_STAGING}.stg_flavours AS
        SELECT
            flavour_id::INTEGER AS flavour_id,
            TRIM(name) AS flavour_name,
            TRIM(description) AS flavour_description,
            generation_date,
            batch_number::INTEGER AS batch_number
        FROM {SCHEMA_RAW}.flavours
    """)
    print_table_info(con, SCHEMA_STAGING, "stg_flavours")

    #stg_recipes
    
    con.execute(f"""
        CREATE OR REPLACE TABLE {SCHEMA_STAGING}.stg_recipes AS
        SELECT
            recipe_id,
            raw_material_id::INTEGER AS raw_material_id,
            raw_material_ratio,
            flavour_id::INTEGER AS flavour_id,
            flavour_ratio,
            ingredient_id::INTEGER AS ingredient_id,
            ingredient_ratio,
            NULLIF(TRIM(heat_process), '') AS heat_process,
            yield AS yield_percentage,
            generation_date,
            batch_number::INTEGER AS batch_number
        FROM {SCHEMA_RAW}.recipes
    """)
    print_table_info(con, SCHEMA_STAGING, "stg_recipes")

    #stg_sales_transactions
    
    con.execute(f"""
        CREATE OR REPLACE TABLE {SCHEMA_STAGING}.stg_sales_transactions AS
        SELECT
            transaction_id::INTEGER AS transaction_id,
            customer_id::INTEGER AS customer_id,
            flavour_id::INTEGER AS flavour_id,
            quantity_liters::INTEGER AS quantity_liters,
            transaction_date,
            UPPER(TRIM(transaction_country)) AS transaction_country,
            TRIM(transaction_town) AS transaction_town,
            TRIM(postal_code) AS postal_code,
            amount_dollar::DOUBLE AS amount_dollars,
            generation_date,
            batch_number::INTEGER AS batch_number
        FROM {SCHEMA_RAW}.sales_transactions
    """)
    print_table_info(con, SCHEMA_STAGING, "stg_sales_transactions")

    con.close()
    print("\nStaging layer complete.\n")


if __name__ == "__main__":
    create_staging_tables()
