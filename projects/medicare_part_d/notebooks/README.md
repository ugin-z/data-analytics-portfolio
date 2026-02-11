# Notebooks

This directory contains the Jupyter notebooks used in the project.
Each notebook has a clearly defined role in the overall data workflow.

## analysis.ipynb
This notebook is used for:
- analytical queries written in SQL via DuckDB,
- answering business questions,
- generating insights for reporting and presentation.

No data preparation or mart construction is performed in this notebook.
All analytics rely exclusively on the mart layer.

## Execution Order
- run 'analysis.ipynb' to perform the analysis.
