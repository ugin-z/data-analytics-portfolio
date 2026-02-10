# Medicare Part D Spending Analysis (2023)

This project analyzes Medicare Part D prescription drug spending patterns for the 2023 calendar year using a production-style data mart architecture.

The goal of the project is to identify key cost drivers at the drug and prescriber levels and to demonstrate applied analytical thinking suitable for Entry / Junior Data Analyst and BI Analyst roles.

**Tableau Dashboards:**
- Price vs Volume Drivers (Scatter): https://public.tableau.com/app/profile/yevhen.zinchenko/viz/MedicarePartDDrugSpending-2023_overview/Overview
- Spending Overview & Concentration (Pareto): https://public.tableau.com/app/profile/yevhen.zinchenko/viz/ParetoAnalysisofTop50DrugsbySpending-2023Pareto/Pareto

## Project Objectives

- Analyze concentration of Medicare Part D spending at the drug level
- Evaluate prescriber-level contribution to total program costs
- Distinguish between price-driven and volume-driven cost drivers
- Demonstrate data modeling, data quality, and SQL-based analytics

## Data Architecture

The project follows a layered data architecture:

raw -> clean -> mart

- raw/: immutable source data
- clean/: cleaned and standardized datasets
- mart/: analytical data marts (source of truth)

All analytical data marts are stored in Parquet format and queried using SQL via DuckDB.

## Analytical Data Marts

The analysis is built on three analytical data marts:

- mart_drug_year  
  Drug-level spending and utilization metrics by year

- mart_prescriber_drug_year  
  Prescriber-level spending within individual drugs

- mart_prescriber_year  
  System-wide prescriber-level spending metrics

## Key Findings

- Medicare Part D spending is highly concentrated at the drug level, with a small set of drugs accounting for a large share of total expenditures.
- At the system level, prescriber-level concentration is low, indicating that overall spending is broadly distributed across prescribers.
- Within individual drugs, spending is often concentrated among a limited number of prescribers.
- A deep dive into the highest-spending drugs shows that elevated total costs are primarily driven by high utilization rather than high cost per claim.

## Repository Structure

src/        - data ingestion, validation, and mart construction  
data/       - raw, clean, and mart datasets (Parquet)  
notebooks/  - mart construction and analytical notebooks  
reports/    - executive summary and analytical findings  

## Tools & Technologies

- Python (pandas)
- DuckDB (SQL analytics)
- Parquet
- Jupyter Notebook

## Notes

- The analysis is limited to data from the 2023 calendar year.
- The year field is added as a structural dimension during mart construction.
- Pandas is used only for data preparation and mart construction.
- All analytical queries are executed using SQL against the mart layer.
- The project emphasizes business interpretation over exploratory analysis.