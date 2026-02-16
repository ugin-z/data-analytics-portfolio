from pathlib import Path
import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[1]
MART_DIR = PROJECT_ROOT / 'data' / 'mart'

paths = {
    'mart_citations_month': MART_DIR / 'mart_citations_month.parquet',
    'mart_citations_year_month': MART_DIR / 'mart_citations_year_month.parquet',
    'mart_citations_year': MART_DIR / 'mart_citations_year.parquet',
    'mart_state_year': MART_DIR / 'mart_state_year.parquet'
}

print('PROJECT_ROOT:', PROJECT_ROOT)
print('MART_DIR:', MART_DIR)

for name, path in paths.items():
    print(f'{name}: {path} -> exists={path.exists()}')

con = duckdb.connect(str(MART_DIR / 'sfmta_parking_citations.duckdb'))

for name, path in paths.items():
    if not path.exists():
        raise FileNotFoundError(f'{name}: file not found: {path}')
    con.execute(f"""
        create or replace view {name} AS
        select *
        from read_parquet('{path.as_posix()}');
    """)
    print(f'Created/updated table: {name}')

con.close()
print('Done.')