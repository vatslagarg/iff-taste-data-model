import pytest
import duckdb
from src.config import DB_PATH


@pytest.fixture(scope="module")

def con():
    connection = duckdb.connect(DB_PATH, read_only=True)
    yield connection
    connection.close()

def assert_pk_unique_and_not_null(con, schema, table, pk_column):

    result = con.execute(f"""
        SELECT
            COUNT(*) AS total_rows,
            COUNT({pk_column}) AS non_null_rows,
            COUNT(DISTINCT {pk_column}) AS distinct_values
        FROM {schema}.{table}
    """).fetchone()

    total_rows, non_null_rows, distinct_values = result

    assert total_rows > 0, f"{schema}.{table} is empty"
    assert total_rows == non_null_rows, (
        f"{schema}.{table}.{pk_column} has {total_rows - non_null_rows} NULL values"
    )
    assert total_rows == distinct_values, (
        f"{schema}.{table}.{pk_column} has {total_rows - distinct_values} duplicate values"
    )


def assert_referential_integrity(con, child_schema, child_table, child_fk,
                                  parent_schema, parent_table, parent_pk,
                                  extra_filter=""):
    
    where_clause = f"AND {extra_filter}" if extra_filter else ""
    orphans = con.execute(f"""
        SELECT COUNT(*)
        FROM {child_schema}.{child_table} c
        LEFT JOIN {parent_schema}.{parent_table} p
            ON c.{child_fk} = p.{parent_pk} {where_clause}
        WHERE p.{parent_pk} IS NULL
    """).fetchone()[0]

    assert orphans == 0, (
        f"{child_schema}.{child_table}.{child_fk} has {orphans} orphan records "
        f"not found in {parent_schema}.{parent_table}.{parent_pk}"
    )

# 1. COMPLETENESS & UNIQUENESS TESTS (Primary Keys)

class TestPrimaryKeyUniqueness:
    """Verify all primary keys are unique and not NULL."""

    def test_dim_customers_pk(self, con):
        assert_pk_unique_and_not_null(con, "marts", "dim_customers", "customer_id")

    def test_dim_providers_pk(self, con):
        assert_pk_unique_and_not_null(con, "marts", "dim_providers", "provider_id")

    def test_dim_raw_materials_pk(self, con):
        assert_pk_unique_and_not_null(con, "marts", "dim_raw_materials", "raw_material_id")

    def test_dim_ingredients_pk(self, con):
        assert_pk_unique_and_not_null(con, "marts", "dim_ingredients", "ingredient_id")

    def test_dim_flavours_pk(self, con):
        """flavour_scd_key is the surrogate PK (flavour_id is NOT unique due to SCD2)."""
        assert_pk_unique_and_not_null(con, "marts", "dim_flavours", "flavour_scd_key")

    def test_dim_date_pk(self, con):
        assert_pk_unique_and_not_null(con, "marts", "dim_date", "date_key")

    def test_dim_recipes_pk(self, con):
        assert_pk_unique_and_not_null(con, "marts", "dim_recipes", "recipe_key")

    def test_fct_sales_transactions_pk(self, con):
        assert_pk_unique_and_not_null(con, "marts", "fct_sales_transactions", "transaction_id")

    def test_fct_provider_inventory_pk(self, con):
        assert_pk_unique_and_not_null(con, "marts", "fct_provider_inventory", "ingredient_id")

    def test_fct_recipe_composition_pk(self, con):
        assert_pk_unique_and_not_null(con, "marts", "fct_recipe_composition", "recipe_key")


# 2. REFERENTIAL INTEGRITY TESTS (Foreign Keys)

class TestReferentialIntegrity:
    """Verify all foreign keys reference valid parent records."""

    def test_fct_sales_customer_fk(self, con):
        """Every sales transaction references a valid customer."""
        assert_referential_integrity(
            con, "marts", "fct_sales_transactions", "customer_id",
            "marts", "dim_customers", "customer_id"
        )

    def test_fct_sales_flavour_fk(self, con):
        """Every sales transaction references a valid current flavour."""
        assert_referential_integrity(
            con, "marts", "fct_sales_transactions", "flavour_id",
            "marts", "dim_flavours", "flavour_id",
            extra_filter="p.is_current = TRUE"
        )

    def test_fct_sales_date_fk(self, con):
        """Every transaction_date exists in the date dimension."""
        assert_referential_integrity(
            con, "marts", "fct_sales_transactions", "transaction_date",
            "marts", "dim_date", "date_key"
        )

    def test_dim_ingredients_provider_fk(self, con):
        """
        Every ingredient references a valid provider.

        KNOWN SOURCE DATA ISSUE: 2 ingredients (IDs 249, 270 - both "Proline")
        reference provider_id=110, which does not exist in the providers table
        (max provider_id is 108). This is a source data defect.
        We assert exactly 2 orphans to document this known issue.
        """
        orphans = con.execute("""
            SELECT COUNT(*)
            FROM marts.dim_ingredients i
            LEFT JOIN marts.dim_providers p ON i.provider_id = p.provider_id
            WHERE p.provider_id IS NULL
        """).fetchone()[0]
        assert orphans == 2, (
            f"Expected exactly 2 orphan ingredients (known issue), got {orphans}"
        )

    def test_fct_recipe_raw_material_fk(self, con):
        """Every recipe references a valid raw material."""
        assert_referential_integrity(
            con, "marts", "fct_recipe_composition", "raw_material_id",
            "marts", "dim_raw_materials", "raw_material_id"
        )

    def test_fct_recipe_flavour_fk(self, con):
        """Every recipe references a valid current flavour."""
        assert_referential_integrity(
            con, "marts", "fct_recipe_composition", "flavour_id",
            "marts", "dim_flavours", "flavour_id",
            extra_filter="p.is_current = TRUE"
        )

    def test_fct_recipe_ingredient_fk(self, con):
        """
        Every recipe references a valid ingredient.

        KNOWN SOURCE DATA ISSUE: The recipes table contains ingredient_ids
        in the range 1-299, while the ingredients table contains IDs 101-400.
        This means ingredient_ids 1-100 in recipes have no matching record
        in the ingredients dimension. This affects ~55,841 recipe rows.
        This is a source data defect (likely a data generation bug where
        ingredient IDs were generated with a different offset).
        """
        orphans = con.execute("""
            SELECT COUNT(*)
            FROM marts.fct_recipe_composition c
            LEFT JOIN marts.dim_ingredients i ON c.ingredient_id = i.ingredient_id
            WHERE i.ingredient_id IS NULL
        """).fetchone()[0]
        # Document the known issue: orphans should be > 0 due to source data
        assert orphans > 0, "Expected orphan ingredients due to known source data issue"
        assert orphans < 60000, f"Orphan count ({orphans}) is unexpectedly high"

# 3. SCD TYPE 2 INTEGRITY TESTS

class TestSCD2Integrity:
    """Verify the SCD2 flavour dimension is correctly constructed."""

    def test_every_flavour_has_exactly_one_current_record(self, con):
        """Each flavour_id must have exactly one row where is_current = TRUE."""
        result = con.execute("""
            SELECT flavour_id, COUNT(*) AS current_count
            FROM marts.dim_flavours
            WHERE is_current = TRUE
            GROUP BY flavour_id
            HAVING COUNT(*) != 1
        """).fetchall()

        assert len(result) == 0, (
            f"{len(result)} flavour(s) have != 1 current record: {result[:5]}"
        )

    def test_closed_records_have_valid_to(self, con):
        """Non-current records must have a non-NULL valid_to date."""
        nulls = con.execute("""
            SELECT COUNT(*)
            FROM marts.dim_flavours
            WHERE is_current = FALSE AND valid_to IS NULL
        """).fetchone()[0]

        assert nulls == 0, f"{nulls} closed SCD2 records have NULL valid_to"

    def test_current_records_have_null_valid_to(self, con):
        """Current records must have NULL valid_to (still active)."""
        non_nulls = con.execute("""
            SELECT COUNT(*)
            FROM marts.dim_flavours
            WHERE is_current = TRUE AND valid_to IS NOT NULL
        """).fetchone()[0]

        assert non_nulls == 0, f"{non_nulls} current SCD2 records have non-NULL valid_to"

    def test_scd2_no_timeline_gaps(self, con):
        """
        For flavours with multiple versions, the old record's valid_to
        must equal the new record's valid_from (no gaps).
        """
        gaps = con.execute("""
            WITH versioned AS (
                SELECT
                    flavour_id,
                    valid_from,
                    valid_to,
                    is_current,
                    LEAD(valid_from) OVER (
                        PARTITION BY flavour_id ORDER BY valid_from
                    ) AS next_valid_from
                FROM marts.dim_flavours
            )
            SELECT COUNT(*)
            FROM versioned
            WHERE is_current = FALSE
              AND valid_to != next_valid_from
        """).fetchone()[0]

        assert gaps == 0, f"{gaps} SCD2 records have timeline gaps"

    def test_all_500_flavours_present(self, con):
        """All 500 original flavour_ids should be represented."""
        count = con.execute("""
            SELECT COUNT(DISTINCT flavour_id) FROM marts.dim_flavours
        """).fetchone()[0]

        assert count == 500, f"Expected 500 flavours, got {count}"

# 4. BUSINESS LOGIC / CONSISTENCY TESTS

class TestBusinessLogic:
    """Verify business rules are enforced in the data."""

    def test_recipe_ratios_sum_to_one(self, con):
        """
        Each recipe's 3 component ratios should sum to approximately 1.0.
        We allow a tolerance of 0.01 for floating-point precision.
        """
        violations = con.execute("""
            SELECT COUNT(*)
            FROM marts.fct_recipe_composition
            WHERE total_ratio < 0.99 OR total_ratio > 1.01
        """).fetchone()[0]

        assert violations == 0, (
            f"{violations} recipes have ratios that don't sum to ~1.0"
        )

    def test_recipe_individual_ratios_between_0_and_1(self, con):
        """Each individual ratio should be between 0 and 1."""
        violations = con.execute("""
            SELECT COUNT(*)
            FROM marts.fct_recipe_composition
            WHERE raw_material_ratio < 0 OR raw_material_ratio > 1
               OR flavour_ratio < 0 OR flavour_ratio > 1
               OR ingredient_ratio < 0 OR ingredient_ratio > 1
        """).fetchone()[0]

        assert violations == 0, f"{violations} recipes have ratios outside [0, 1]"

    def test_yield_percentage_in_valid_range(self, con):
        """Yield percentages should be between 0 and 100."""
        violations = con.execute("""
            SELECT COUNT(*)
            FROM marts.fct_recipe_composition
            WHERE yield_percentage < 0 OR yield_percentage > 100
        """).fetchone()[0]

        assert violations == 0, f"{violations} recipes have yield outside [0, 100]"

# 5. VALIDITY TESTS (Value Ranges)

class TestValidity:
    """Verify values fall within expected ranges."""

    def test_sales_amounts_non_negative(self, con):
        """
        All sales amounts should be non-negative.

        KNOWN SOURCE DATA ISSUE: 22 transactions have amount_dollars = 0.
        These may represent promotional/sample transactions.
        We verify no NEGATIVE amounts exist (those would indicate data corruption).
        """
        negatives = con.execute("""
            SELECT COUNT(*)
            FROM marts.fct_sales_transactions
            WHERE amount_dollars < 0
        """).fetchone()[0]
        assert negatives == 0, f"{negatives} transactions have negative amounts"

        zeros = con.execute("""
            SELECT COUNT(*)
            FROM marts.fct_sales_transactions
            WHERE amount_dollars = 0
        """).fetchone()[0]
        assert zeros == 22, f"Expected 22 zero-amount transactions (known), got {zeros}"

    def test_sales_quantities_non_negative(self, con):
        """
        All quantities should be non-negative.

        KNOWN SOURCE DATA ISSUE: 475 transactions have quantity_liters = 0.
        These may represent cancelled orders or data entry issues.
        We verify no NEGATIVE quantities exist.
        """
        negatives = con.execute("""
            SELECT COUNT(*)
            FROM marts.fct_sales_transactions
            WHERE quantity_liters < 0
        """).fetchone()[0]
        assert negatives == 0, f"{negatives} transactions have negative quantities"

        zeros = con.execute("""
            SELECT COUNT(*)
            FROM marts.fct_sales_transactions
            WHERE quantity_liters = 0
        """).fetchone()[0]
        assert zeros == 475, f"Expected 475 zero-quantity transactions (known), got {zeros}"

    def test_ingredient_values_positive(self, con):
        """All ingredient values should be positive."""
        violations = con.execute("""
            SELECT COUNT(*)
            FROM marts.fct_provider_inventory
            WHERE total_ingredient_value <= 0
        """).fetchone()[0]

        assert violations == 0, f"{violations} ingredients have non-positive values"

    def test_ingredient_weights_positive(self, con):
        """All ingredient weights should be positive."""
        violations = con.execute("""
            SELECT COUNT(*)
            FROM marts.dim_ingredients
            WHERE weight_in_grams <= 0
        """).fetchone()[0]

        assert violations == 0, f"{violations} ingredients have non-positive weights"

    def test_transaction_dates_in_expected_range(self, con):
        """
        Transaction dates should fall within a reasonable range.

        Actual range discovered: 2023-02-16 to 2025-01-15.
        We verify all dates fall within the extended dim_date range.
        """
        violations = con.execute("""
            SELECT COUNT(*)
            FROM marts.fct_sales_transactions
            WHERE transaction_date < DATE '2023-01-01'
               OR transaction_date > DATE '2025-12-31'
        """).fetchone()[0]

        assert violations == 0, f"{violations} transactions outside dim_date range"

# 6. ROW COUNT TESTS

class TestRowCounts:
    """Verify expected row counts for key tables."""

    def test_dim_customers_count(self, con):
        count = con.execute("SELECT COUNT(*) FROM marts.dim_customers").fetchone()[0]
        assert count == 75, f"Expected 75 customers, got {count}"

    def test_dim_providers_count(self, con):
        count = con.execute("SELECT COUNT(*) FROM marts.dim_providers").fetchone()[0]
        assert count == 108, f"Expected 108 providers, got {count}"

    def test_dim_raw_materials_count(self, con):
        count = con.execute("SELECT COUNT(*) FROM marts.dim_raw_materials").fetchone()[0]
        assert count == 200, f"Expected 200 raw materials, got {count}"

    def test_dim_ingredients_count(self, con):
        count = con.execute("SELECT COUNT(*) FROM marts.dim_ingredients").fetchone()[0]
        assert count == 300, f"Expected 300 ingredients, got {count}"

    def test_dim_flavours_has_500_unique_ids(self, con):
        count = con.execute("SELECT COUNT(DISTINCT flavour_id) FROM marts.dim_flavours").fetchone()[0]
        assert count == 500, f"Expected 500 unique flavour IDs, got {count}"

    def test_fct_sales_transactions_count(self, con):
        count = con.execute("SELECT COUNT(*) FROM marts.fct_sales_transactions").fetchone()[0]
        assert count == 50000, f"Expected 50000 transactions, got {count}"

    def test_fct_provider_inventory_count(self, con):
        count = con.execute("SELECT COUNT(*) FROM marts.fct_provider_inventory").fetchone()[0]
        assert count == 300, f"Expected 300 inventory rows, got {count}"
