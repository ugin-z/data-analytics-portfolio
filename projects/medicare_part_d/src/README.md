# Source Code

This directory contains the source code used to ingest, validate, transform,
and prepare Medicare Part D data for analytical use.

The code in this directory supports data preparation only and is not used for
analytical querying or reporting.

## Structure Overview

### ingest/
Code responsible for loading and structuring raw Medicare Part D data and
related reference datasets.

- partd.py: ingestion logic for Medicare Part D source data
- provider.py: ingestion and preparation of provider-related data

### dq_checks.py
Data quality validation logic, including:
- grain validation,
- duplicate detection,
- logical consistency checks.

### transform.py
Transformation logic used to construct analytical data marts from the cleaned
data layer, including aggregation and structural enrichment.

## build_marts.py
This notebook is responsible for:
- loading cleaned source data,
- performing data quality checks,
- constructing analytical data marts,
- saving the marts in Parquet format.

## Design Principles
- The 'src/' layer is responsible only for data preparation.
- Pandas is used exclusively for ingestion, validation, and transformation.
- All analytical queries are performed separately using SQL against the mart layer.
