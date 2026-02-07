# Data Directory

This directory contains all data used in the Medicare Part D (2023) analysis,
organized by processing stage.

## raw/
Raw source files as obtained from the original CMS Medicare Part D dataset.
These files are not modified and serve as the immutable source of truth.

## clean/
Cleaned and standardized datasets stored in Parquet format.
This layer includes data cleaning, normalization, and structural fixes.
No analytical aggregations are applied at this stage.

## mart/
Analytical data marts stored in Parquet format.
These marts represent curated, analysis-ready datasets and serve as the
single source of truth for all downstream SQL analytics.

All analytical queries are performed exclusively against the mart layer
using SQL via DuckDB.
