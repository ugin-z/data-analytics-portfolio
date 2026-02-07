from pathlib import Path
import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[1]
MART_DIR = PROJECT_ROOT / "data" / "mart"

paths = {
    "mart_drug_year": MART_DIR / "mart_drug_year_2026-02-01.parquet",
    "mart_prescriber_drug_year": MART_DIR / "mart_prescriber_drug_year_2026-02-01.parquet",
    "mart_prescriber_year": MART_DIR / "mart_prescriber_year_2026-02-01.parquet",
}

print("PROJECT_ROOT:", PROJECT_ROOT)
print("MART_DIR:", MART_DIR)

for name, path in paths.items():
    print(f"{name}: {path} -> exists={path.exists()}")

con = duckdb.connect(str(PROJECT_ROOT / "medicare_part_d.duckdb"))

for name, path in paths.items():
    if not path.exists():
        raise FileNotFoundError(f"{name}: file not found: {path}")
    con.execute(f"""
        CREATE OR REPLACE TABLE {name} AS
        SELECT *
        FROM read_parquet('{path.as_posix()}');
    """)
    print(f"Created/updated table: {name}")

con.close()
print("Done.")

