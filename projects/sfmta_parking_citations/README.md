# SFMTA Parking Citations Analytics (2021–2025)

End-to-end analytics project analyzing San Francisco parking citation data using a structured data pipeline and BI dashboards.

---

## Overview

This project evaluates:

- 2025 revenue performance vs historical benchmark (2021–2024)
- Revenue concentration by violation type (Pareto analysis)
- Monthly and YTD revenue trends

Interactive dashboards are available on Tableau Public.

---

## Data Pipeline

The project follows a layered model:

raw -> clean -> mart -> analytics

- Citation-level raw dataset (1 row = 1 citation)
- Data cleaning and validation in Python
- Pre-aggregated marts stored in Parquet
- Analytical queries in DuckDB
- Visualization in Tableau Public

---

## Repository Structure

"""
data/      -> raw, clean, and mart layers  
src/       -> data pipeline scripts  
sql/       -> analytical queries  
reports/   -> business analysis summary  
"""

---

## Run

Install dependencies:
"""
pip install -r requirements.txt
"""
Build marts:
"""
python src/build_marts.py
"""

## Tableau Dashboard

**SFParkingCitations-Overview**

Tableau Public:  
- Revenue Performance vs Historical Benchmark
  https://public.tableau.com/app/profile/yevhen.zinchenko/viz/SF_Parking_17712149962210/2025RevenuePerformancevsHistoricalBenchmark

- Revenue Concentration Analysis
  https://public.tableau.com/app/profile/yevhen.zinchenko/viz/RevenueConcentration/RevenueConcentrationAnalysis-2025

- San Francisco Parking Citations (2021-2025)
  https://public.tableau.com/app/profile/yevhen.zinchenko/viz/SFParkingCitations/SFParkingCitations-Overview

---

## Tech Stack

Python • DuckDB • SQL • Parquet • Tableau
