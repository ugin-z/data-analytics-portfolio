# Notebooks

This directory contains the Jupyter notebooks used in the project.
Each notebook has a clearly defined role in the overall data workflow.

## build_marts.ipynb
This notebook is responsible for:
- loading cleaned source data,
- performing data quality checks,
- constructing analytical data marts,
- saving the marts in Parquet format.

This notebook should be executed first.

## analysis.ipynb
This notebook is used for:
- analytical queries written in SQL via DuckDB,
- answering business questions,
- generating insights for reporting and presentation.

No data preparation or mart construction is performed in this notebook.
All analytics rely exclusively on the mart layer.

## Execution Order
1. Run 'build_marts.ipynb' to generate the data marts.
2. Run 'analysis.ipynb' to perform the analysis.
