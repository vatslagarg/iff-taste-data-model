import os
from src.config import RAW_DATA_DIR, SCHEMA_RAW, CSV_FILES
from src.utils import get_connection, create_schema_if_not_exists, print_table_info


def load_raw_data():

    print("STEP 1: Loading raw CSV data into DuckDB")

    con = get_connection()
    create_schema_if_not_exists(con, SCHEMA_RAW)

    for table_name, csv_filename in CSV_FILES.items():
        csv_path = os.path.join(RAW_DATA_DIR, csv_filename)

        if not os.path.exists(csv_path):
            print(f"  WARNING: {csv_path} not found, skipping.")
            continue
        
        con.execute(f"""
            CREATE OR REPLACE TABLE {SCHEMA_RAW}.{table_name} AS
            SELECT * FROM read_csv_auto('{csv_path}')
        """)

        print_table_info(con, SCHEMA_RAW, table_name)

    con.close()
    print("\nRaw layer complete.\n")


if __name__ == "__main__":
    load_raw_data()
