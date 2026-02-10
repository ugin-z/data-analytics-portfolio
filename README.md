# Data Analytics Portfolio - Yevhen (Eugene) Zinchenko

Entry–Junior Data Analyst / BI Analyst / Analytics Engineer  
Focus: analytical thinking, data quality, business-oriented metrics

-----

## About

This repository contains a set of analytical case studies demonstrating 
how I approach data analysis in a structured, production-oriented way.

The focus is not only on writing code, but on:
- understanding the business problem
- validating data reliability
- selecting appropriate analytical methods
- explaining metric logic and assumptions
- translating analysis into actionable insights

-----

## Skills & Tools

### Python & Data Analysis
- pandas, numpy
- data cleaning and transformation
- groupby / agg / transform patterns
- window-style calculations (rank, shift, cumsum, pct_change)
- rolling metrics (row-based and time-based)
- datetime and period-based analysis
- data validation and sanity checks using assertions

### SQL
- analytical queries for business metrics
- GROUP BY vs window functions
- joins and join validation
- cohort and retention analysis
- translating SQL logic into pandas pipelines

### API & Data Ingestion
- REST APIs using requests
- JSON normalization into tabular format
- API to pandas analytical pipelines
- schema and consistency validation

### Visualization & BI
- matplotlib for sanity checks and quick validation
- KPI-oriented visualizations
- Tableau dashboards (junior level)

-----

## Projects

### Medicare Part D - Sales Analysis with Rolling Metrics

Tools: Python, pandas, requests, SQL
Directory: `projects/medicare_part_d`

Focus:
- analytical data modeling (raw -> clean -> mart)
- explicit grain definition and validation
- data quality checks (duplicates, ranges, control totals)
- drug spending analysis with price vs volume decomposition
- concentration and distribution analysis using Pareto and scatter-based exploration, 
with BI communication of insights

This project analyzes U.S. Medicare Part D drug spending data, covering end-to-end ingestion, 
transformation, data quality validation, and analytical reporting.

-----

### SFMTA Parking Citations - Cohort and Retention Analysis

Tools: Python, pandas, SQL
Directory: `projects/sfmta_parking_citations`

Focus:

- cohort construction based on citation behavior
- repeat violation and retention-style metrics
- grain definition and aggregation validation
- consistency checks between SQL and pandas results
- city-level operational analytics

This project is built on San Francisco parking citation data and focuses on 
behavioral patterns, repeat violations, and analytical consistency across tools.

-----

### U.S. Federal Contracts - Data Quality and Market Concentration Analysis

Tools: Python, pandas, SQL
Directory: `projects/us_federal_contracts`

Focus:
- schema and data type validation
- duplicate detection using business keys
- range and domain checks
- time coverage validation
- aggregate reconciliation
- vendor concentration and spend distribution analysis

This project analyzes FY 2025 U.S. federal prime contract awards across all agencies, 
with a strong emphasis on data quality, analytical correctness, and market-level insights.

-----

## Notes on Data

Raw data files are intentionally not stored in this repository.  
Projects use publicly available datasets.  
Data assumptions and structure are documented in each project README.

-----

## Contact
LinkedIn: https://www.linkedin.com/in/eugene-zi/  
GitHub Portfolio: https://github.com/ugin-z/data-analytics-portfolio

Preferred name: Eugene
