# IFF Supply Chain - Dimensional Data Model

A Python + DuckDB data pipeline that transforms 7 raw CSV files from the IFF supply chain into a dimensional data model (star schema) suitable for analytical dashboarding.

# Architecture

The pipeline follows a **layered architecture** with 4 distinct layers, each serving a specific purpose:

```
┌─────────────────────────────────────────────────────────────┐
│  data/raw/*.csv  (7 CSV source files)                       │
└───────────────────────┬─────────────────────────────────────┘
                        ▼
┌─────────────────────────────────────────────────────────────┐
│  RAW LAYER                                                  │
│  Load CSVs as-is into DuckDB. No transformations.           │
│  Purpose: Faithful copy of source data.                     │
└───────────────────────┬─────────────────────────────────────┘
                        ▼
┌─────────────────────────────────────────────────────────────┐
│  STAGING LAYER                                              │
│  Trim whitespace, parse dates, rename columns, cast types.  │
│  Purpose: Clean, standardized data. No business logic.      │
└───────────────────────┬─────────────────────────────────────┘
                        ▼
┌─────────────────────────────────────────────────────────────┐
│  INTERMEDIATE LAYER                                         │
│  Deduplication, SCD Type 2, enrichment.                     │
│  Purpose: Business rules applied.                           │
└───────────────────────┬─────────────────────────────────────┘
                        ▼
┌─────────────────────────────────────────────────────────────┐
│  MARTS LAYER                                                │
│  Final dimensions + fact tables.                            │
│  Purpose: Star schema ready for analyst queries.            │
└─────────────────────────────────────────────────────────────┘
```

# Dimensional Model

# Dimensions (7 tables)

| Table | Key | Rows | Description |
|-------|-----|------|-------------|
| `dim_customers` | `customer_id` | 75 | Customer name, city, country |
| `dim_providers` | `provider_id` | 108 | Provider name, city, country (deduplicated) |
| `dim_raw_materials` | `raw_material_id` | 200 | Raw material names |
| `dim_ingredients` | `ingredient_id` | 300 | Ingredient name, formula, weight, cost, provider FK |
| `dim_flavours` | `flavour_scd_key` | 599 | **SCD Type 2** - tracks description changes across batches |
| `dim_recipes` | `recipe_key` | 166,722 | Recipe header attributes |
| `dim_date` | `date_key` | 1,096 | Calendar dimension (2023-2025) with year, quarter, month |

# Facts (3 tables)

| Table | Grain | Rows | Key Measures |
|-------|-------|------|--------------|
| `fct_sales_transactions` | 1 per transaction | 50,000 | quantity_liters, amount_dollars |
| `fct_provider_inventory` | 1 per ingredient | 300 | weight, cost, total_value |
| `fct_recipe_composition` | 1 per recipe | 166,722 | component ratios, yield |


# Prerequisites

**Local Python**
- Python 3.9+
- pip

# Quick Start

1) Clone the repository:

    git clone https://github.com/vatslagarg/iff-taste-data-model.git
    cd iff-taste-data-model   #(or your repo folder name)

2) Create and activate a virtual environment:

    **macOS / Linux** 
    python3 -m venv .venv
    source .venv/bin/activate

    **Windows (PowerShell)**
    python -m venv .venv
    .venv\Scripts\Activate.ps1

3) Install Dependencies:

    pip install -r requirements.txt

4) Run the Pipeline:

    python scripts/run_pipeline.py

    **The pipeline will:**
    1. Load raw CSVs into DuckDB
    2. Clean and standardize data (staging)
    3. Apply business logic (intermediate)
    4. Create dimensional model (marts)
    5. Run 37 data quality tests

5) After execution, the following file will be generated: **iff_supply_chain.duckdb**

6) Query the Dimensional Data Model:
    duckdb iff_supply_chain.duckdb

7) Explore the Model:

    **Inside DuckDB**
    SHOW ALL TABLES;

    **Preview data**
    # Top 5 customers by total sales
    SELECT c.customer_name, SUM(s.amount_dollars) as total_sales
    FROM marts.fct_sales_transactions s
    JOIN marts.dim_customers c ON s.customer_id = c.customer_id
    GROUP BY 1 ORDER BY 2 DESC LIMIT 5;

    # Provider inventory value by country
    SELECT provider_country, SUM(total_ingredient_value) as total_value
    FROM marts.fct_provider_inventory
    GROUP BY 1 ORDER BY 2 DESC;

    # Flavour description history
    SELECT flavour_name, flavour_description, valid_from, valid_to, is_current
    FROM marts.dim_flavours
    WHERE flavour_id = 1
    ORDER BY valid_from;

8) Exit:
   .quit

# Data Quality

37 automated tests cover:
- **Primary key uniqueness** (10 tests) - every table's PK is unique and not null
- **Referential integrity** (7 tests) - all foreign keys point to valid parent records
- **SCD2 integrity** (5 tests) - no gaps, overlaps, or missing current records
- **Business logic** (3 tests) - recipe ratios sum to 1.0, yield in valid range
- **Value validity** (5 tests) - amounts, quantities, weights are reasonable
- **Row counts** (7 tests) - expected record counts

