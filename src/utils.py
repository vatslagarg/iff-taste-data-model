import duckdb
from src.config import DB_PATH


def get_connection() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(DB_PATH)


def get_parse_date_sql(column_name: str) -> str:
    return f"""COALESCE(
        TRY_STRPTIME({column_name}::VARCHAR, '%m/%d/%Y'),
        TRY_STRPTIME({column_name}::VARCHAR, '%m/%d/%y'),
        TRY_STRPTIME({column_name}::VARCHAR, '%-d-%b-%y')
    )::DATE"""


def create_schema_if_not_exists(con: duckdb.DuckDBPyConnection, schema_name: str):
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")


def print_table_info(con: duckdb.DuckDBPyConnection, schema: str, table: str):
    count = con.execute(f"SELECT COUNT(*) FROM {schema}.{table}").fetchone()[0]
    print(f"  {schema}.{table}: {count:,} rows")
