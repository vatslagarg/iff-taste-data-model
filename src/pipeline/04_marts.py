from src.config import SCHEMA_STAGING, SCHEMA_INTERMEDIATE, SCHEMA_MARTS
from src.utils import get_connection, create_schema_if_not_exists, print_table_info


def create_mart_tables():
    
    print("STEP 4: Creating mart tables (dimensional model)")

    con = get_connection()
    create_schema_if_not_exists(con, SCHEMA_MARTS)

    #dim_customers

    con.execute(f"""
        CREATE OR REPLACE TABLE {SCHEMA_MARTS}.dim_customers AS
        SELECT
            customer_id,
            customer_name,
            customer_city,
            customer_country
        FROM {SCHEMA_INTERMEDIATE}.int_customers
    """)
    print_table_info(con, SCHEMA_MARTS, "dim_customers")

    #dim_providers
    con.execute(f"""
        CREATE OR REPLACE TABLE {SCHEMA_MARTS}.dim_providers AS
        SELECT
            provider_id,
            provider_name,
            provider_city,
            provider_country
        FROM {SCHEMA_INTERMEDIATE}.int_providers
    """)
    print_table_info(con, SCHEMA_MARTS, "dim_providers")

    #dim_raw_materials
    # Only batch 1 exists, no dedup needed. Source from staging directly.
    con.execute(f"""
        CREATE OR REPLACE TABLE {SCHEMA_MARTS}.dim_raw_materials AS
        SELECT
            raw_material_id,
            raw_material_name
        FROM {SCHEMA_STAGING}.stg_raw_materials
    """)
    print_table_info(con, SCHEMA_MARTS, "dim_raw_materials")

    #dim_ingredients
    #Includes a computed total_ingredient_value (weight * cost_per_gram).
    #Retains provider_id as a foreign key to dim_providers.
    con.execute(f"""
        CREATE OR REPLACE TABLE {SCHEMA_MARTS}.dim_ingredients AS
        SELECT
            ingredient_id,
            ingredient_name,
            chemical_formula,
            weight_in_grams,
            cost_per_gram,
            ROUND(weight_in_grams * cost_per_gram, 2) AS total_ingredient_value,
            provider_id
        FROM {SCHEMA_STAGING}.stg_ingredients
    """)
    print_table_info(con, SCHEMA_MARTS, "dim_ingredients")

    #dim_flavours (SCD Type 2)
    #This is the only dimension with historical tracking.
    #WHERE is_current = TRUE -> to get the latest description
    con.execute(f"""
        CREATE OR REPLACE TABLE {SCHEMA_MARTS}.dim_flavours AS
        SELECT
            flavour_scd_key,
            flavour_id,
            flavour_name,
            flavour_description,
            valid_from,
            valid_to,
            is_current
        FROM {SCHEMA_INTERMEDIATE}.int_flavours_scd2
    """)
    print_table_info(con, SCHEMA_MARTS, "dim_flavours")

    #dim_recipes

    con.execute(f"""
        CREATE OR REPLACE TABLE {SCHEMA_MARTS}.dim_recipes AS
        SELECT
            recipe_key,
            recipe_id,
            heat_process,
            yield_percentage,
            batch_number
        FROM {SCHEMA_INTERMEDIATE}.int_recipes
    """)
    print_table_info(con, SCHEMA_MARTS, "dim_recipes")

    #dim_date

    con.execute(f"""
        CREATE OR REPLACE TABLE {SCHEMA_MARTS}.dim_date AS
        WITH date_series AS (
            SELECT UNNEST(
                generate_series(DATE '2023-01-01', DATE '2025-12-31', INTERVAL 1 DAY)
            ) AS date_key
        )
        SELECT
            date_key::DATE AS date_key,
            EXTRACT(YEAR FROM date_key)::INTEGER AS year,
            EXTRACT(QUARTER FROM date_key)::INTEGER AS quarter,
            EXTRACT(MONTH FROM date_key)::INTEGER AS month,
            EXTRACT(DAY FROM date_key)::INTEGER AS day_of_month,
            EXTRACT(DOW FROM date_key)::INTEGER AS day_of_week,
            STRFTIME(date_key, '%B') AS month_name,
            STRFTIME(date_key, '%A') AS day_name,
            EXTRACT(YEAR FROM date_key)::VARCHAR || '-Q' ||
                EXTRACT(QUARTER FROM date_key)::VARCHAR AS year_quarter
        FROM date_series
    """)
    print_table_info(con, SCHEMA_MARTS, "dim_date")

    #FACT TABLES
    # Foreign keys: customer_id -> dim_customers, flavour_id -> dim_flavours, transaction_date -> dim_date

    con.execute(f"""
        CREATE OR REPLACE TABLE {SCHEMA_MARTS}.fct_sales_transactions AS
        SELECT
            transaction_id,
            customer_id,
            flavour_id,
            quantity_liters,
            transaction_date,
            transaction_country,
            transaction_town,
            postal_code,
            amount_dollars,
            EXTRACT(YEAR FROM transaction_date)::INTEGER AS transaction_year,
            EXTRACT(QUARTER FROM transaction_date)::INTEGER AS transaction_quarter,
            EXTRACT(YEAR FROM transaction_date)::VARCHAR || '-Q' ||
                EXTRACT(QUARTER FROM transaction_date)::VARCHAR AS transaction_year_quarter
        FROM {SCHEMA_STAGING}.stg_sales_transactions
    """)
    print_table_info(con, SCHEMA_MARTS, "fct_sales_transactions")

    #fact_provider_inventory
    
    con.execute(f"""
        CREATE OR REPLACE TABLE {SCHEMA_MARTS}.fct_provider_inventory AS
        SELECT
            i.ingredient_id,
            i.ingredient_name,
            i.chemical_formula,
            i.weight_in_grams,
            i.cost_per_gram,
            ROUND(i.weight_in_grams * i.cost_per_gram, 2) AS total_ingredient_value,
            p.provider_id,
            p.provider_name,
            p.provider_city,
            p.provider_country
        FROM {SCHEMA_STAGING}.stg_ingredients i
        LEFT JOIN {SCHEMA_INTERMEDIATE}.int_providers p
            ON i.provider_id = p.provider_id
    """)
    print_table_info(con, SCHEMA_MARTS, "fct_provider_inventory")

    #fact_recipe_composition

    con.execute(f"""
        CREATE OR REPLACE TABLE {SCHEMA_MARTS}.fct_recipe_composition AS
        SELECT
            recipe_key,
            recipe_id,
            raw_material_id,
            raw_material_ratio,
            flavour_id,
            flavour_ratio,
            ingredient_id,
            ingredient_ratio,
            total_ratio,
            ROUND(raw_material_ratio / NULLIF(total_ratio, 0), 4) AS raw_material_pct,
            ROUND(flavour_ratio / NULLIF(total_ratio, 0), 4) AS flavour_pct,
            ROUND(ingredient_ratio / NULLIF(total_ratio, 0), 4) AS ingredient_pct,
            heat_process,
            yield_percentage,
            batch_number
        FROM {SCHEMA_INTERMEDIATE}.int_recipes
    """)
    print_table_info(con, SCHEMA_MARTS, "fct_recipe_composition")

    con.close()
    print("\nMarts layer complete.\n")


if __name__ == "__main__":
    create_mart_tables()
