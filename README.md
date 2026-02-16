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

### Medicare Part D - Drug Spending Analytics (2023)

Tools: Python, pandas, requests, DuckDB, SQL, Tableau  
Directory: `projects/medicare_part_d`

Focus:

- structured data modeling (raw -> clean -> mart)
- explicit grain definition and validation
- data quality checks (duplicates, ranges, control totals)
- drug spending analysis with price vs volume decomposition
- concentration analysis using Pareto and distribution-based exploration
- BI-ready analytical outputs and dashboard communication

This project analyzes U.S. Medicare Part D drug spending data, covering ingestion from public sources,
structured transformation, data quality validation, and revenue concentration analysis.

-----

### SFMTA Parking Revenue Analytics (2021–2025)

Tools: Python, pandas, DuckDB, SQL, Tableau  
Directory: `projects/sfmta_parking_citations`

Focus:

- revenue benchmarking vs historical average (2021–2024)
- year-to-date and seasonal trend analysis
- Pareto analysis and revenue concentration by violation type
- structured raw -> clean -> mart data modeling
- analytical consistency between SQL and Python outputs

This project analyzes San Francisco parking citation revenue trends and
concentration patterns using a reproducible analytics pipeline and BI dashboards.

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
