import sys
import subprocess
import os
import importlib

# Add project root to path so we can import src modules
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)


def run_step(step_name, module_path, func_name):
    try:
        module = importlib.import_module(module_path)
        func = getattr(module, func_name)
        func()
    except Exception as e:
        print(f"\nERROR in {step_name}: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


def run_tests():

    print("STEP 5: Running data quality tests")
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "src/tests/test_data_quality.py", "-v"],
        cwd=PROJECT_ROOT,
    )
    if result.returncode != 0:
        print("\nERROR: Data quality tests failed!")
        sys.exit(1)


def main():
    # Run pipeline steps in order
    run_step("Raw Data Loading", "src.pipeline.01_load_raw", "load_raw_data")
    run_step("Staging Layer", "src.pipeline.02_staging", "create_staging_tables")
    run_step("Intermediate Layer", "src.pipeline.03_intermediate", "create_intermediate_tables")
    run_step("Marts Layer", "src.pipeline.04_marts", "create_mart_tables")
    run_tests()

    print("Pipeline completed successfully!")
    print(f"\nDatabase file: {os.path.join(PROJECT_ROOT, 'iff_supply_chain.duckdb')}")
    print("\nTo query the database, run:")
    print("duckdb iff_supply_chain.duckdb")
    print("or")
    print("python3 -c \"import duckdb; con = duckdb.connect('iff_supply_chain.duckdb')\"")
    print()


if __name__ == "__main__":
    main()
