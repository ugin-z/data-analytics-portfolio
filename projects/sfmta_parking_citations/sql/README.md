# SQL Layer

This directory contains SQL scripts executed against the DuckDB warehouse.

## Structure

"""
setup/      -> database setup (views over Parquet marts)
analysis/   -> analytical queries (benchmarking, Pareto, trends)
"""

## Execution

Run from the project root:

"""
duckdb data/mart/sfmta_parking_citations.duckdb
.read sql/setup/bootstrap.sql
.read sql/analysis/01_top_violations_by_fines.sql
"""

All analytical queries operate on pre-built mart tables.