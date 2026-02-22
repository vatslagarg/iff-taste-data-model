from src.config import SCHEMA_STAGING, SCHEMA_INTERMEDIATE
from src.utils import get_connection, create_schema_if_not_exists, print_table_info


def create_intermediate_tables():
    
    print("STEP 3: Creating intermediate tables (business logic)")

    con = get_connection()
    create_schema_if_not_exists(con, SCHEMA_INTERMEDIATE)

    con.execute(f"""
        CREATE OR REPLACE TABLE {SCHEMA_INTERMEDIATE}.int_customers AS
        WITH ranked AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY customer_id
                    ORDER BY batch_number DESC, generation_date DESC
                ) AS rn
            FROM {SCHEMA_STAGING}.stg_customers
        )
        SELECT
            customer_id,
            customer_name,
            customer_city,
            customer_country
        FROM ranked
        WHERE rn = 1
    """)
    print_table_info(con, SCHEMA_INTERMEDIATE, "int_customers")

    con.execute(f"""
        CREATE OR REPLACE TABLE {SCHEMA_INTERMEDIATE}.int_providers AS
        WITH ranked AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY provider_id
                    ORDER BY batch_number DESC, generation_date DESC
                ) AS rn
            FROM {SCHEMA_STAGING}.stg_providers
        )
        SELECT
            provider_id,
            provider_name,
            provider_city,
            provider_country
        FROM ranked
        WHERE rn = 1
    """)
    print_table_info(con, SCHEMA_INTERMEDIATE, "int_providers")

    con.execute(f"""
        CREATE OR REPLACE TABLE {SCHEMA_INTERMEDIATE}.int_flavours_scd2 AS
        WITH batch_1 AS (
            SELECT
                flavour_id,
                flavour_name,
                flavour_description,
                generation_date,
                batch_number
            FROM {SCHEMA_STAGING}.stg_flavours
            WHERE batch_number = 1
        ),
        batch_2 AS (
            SELECT
                flavour_id,
                flavour_name,
                flavour_description,
                generation_date,
                batch_number
            FROM {SCHEMA_STAGING}.stg_flavours
            WHERE batch_number = 2
        ),

        -- Compare descriptions between batches
        compared AS (
            SELECT
                b1.flavour_id,
                b1.flavour_name,
                b1.flavour_description AS desc_batch_1,
                b2.flavour_description AS desc_batch_2,
                b1.generation_date AS gen_date_batch_1,
                b2.generation_date AS gen_date_batch_2,
                CASE
                    WHEN b1.flavour_description != b2.flavour_description THEN TRUE
                    ELSE FALSE
                END AS description_changed
            FROM batch_1 b1
            INNER JOIN batch_2 b2 ON b1.flavour_id = b2.flavour_id
        ),

        -- Build SCD2 records
        scd2_records AS (
            -- Old (closed) record for flavours WHERE description CHANGED
            SELECT
                flavour_id,
                flavour_name,
                desc_batch_1 AS flavour_description,
                gen_date_batch_1 AS valid_from,
                gen_date_batch_2 AS valid_to,
                FALSE AS is_current,
                1 AS source_batch_number
            FROM compared
            WHERE description_changed = TRUE

            UNION ALL

            -- New (current) record for flavours WHERE description CHANGED
            SELECT
                flavour_id,
                flavour_name,
                desc_batch_2 AS flavour_description,
                gen_date_batch_2 AS valid_from,
                NULL::DATE AS valid_to,
                TRUE AS is_current,
                2 AS source_batch_number
            FROM compared
            WHERE description_changed = TRUE

            UNION ALL

            -- Single (current) record for flavours WHERE description DID NOT change
            SELECT
                flavour_id,
                flavour_name,
                desc_batch_1 AS flavour_description,
                gen_date_batch_1 AS valid_from,
                NULL::DATE AS valid_to,
                TRUE AS is_current,
                1 AS source_batch_number
            FROM compared
            WHERE description_changed = FALSE
        )

        SELECT
            MD5(flavour_id::VARCHAR || '|' || valid_from::VARCHAR || '|' || source_batch_number::VARCHAR)
                AS flavour_scd_key,
            flavour_id,
            flavour_name,
            flavour_description,
            valid_from,
            valid_to,
            is_current,
            source_batch_number
        FROM scd2_records
    """)
    print_table_info(con, SCHEMA_INTERMEDIATE, "int_flavours_scd2")

    # Print SCD2 stats
    changed = con.execute(f"""
        SELECT COUNT(DISTINCT flavour_id)
        FROM {SCHEMA_INTERMEDIATE}.int_flavours_scd2
        WHERE is_current = FALSE
    """).fetchone()[0]
    total = con.execute(f"""
        SELECT COUNT(DISTINCT flavour_id)
        FROM {SCHEMA_INTERMEDIATE}.int_flavours_scd2
    """).fetchone()[0]
    print(f"  -> {changed} out of {total} flavours had description changes")

    con.execute(f"""
        CREATE OR REPLACE TABLE {SCHEMA_INTERMEDIATE}.int_recipes AS
        SELECT
            MD5(recipe_id || '|' || batch_number::VARCHAR) AS recipe_key,
            recipe_id,
            raw_material_id,
            raw_material_ratio,
            flavour_id,
            flavour_ratio,
            ingredient_id,
            ingredient_ratio,
            (raw_material_ratio + flavour_ratio + ingredient_ratio) AS total_ratio,
            heat_process,
            yield_percentage,
            generation_date,
            batch_number
        FROM {SCHEMA_STAGING}.stg_recipes
    """)
    print_table_info(con, SCHEMA_INTERMEDIATE, "int_recipes")

    con.close()
    print("\nIntermediate layer complete.\n")


if __name__ == "__main__":
    create_intermediate_tables()
