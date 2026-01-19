# Medicare Part D Cost Drivers Analysis

Focus:
- business-oriented analysis (cost drivers, outliers, geographic patterns)
- strong data quality practices
- reproducible data ingestion and transformation
- and clear communication of results

-----

## Project Goals

### Business Goal
The main business goal of this project is to identify the key cost drivers in Medicare Part D.
Specifically, the project aims to understand which drugs, prescribers, and geographic regions contribute the most to total drug costs, and whether high costs are driven by prescription volume or by high cost per claim.

### Analytical Goals
- Analyze total drug costs and cost per claim across drugs, prescribers, and states.
- Identify cost outliers using percentile-based analysis.
- Compare volume-driven versus price-driven cost patterns.
- Explore geographic and provider-level variations in Medicare Part D spending.
- Summarize findings in a clear and interpretable way for non-technical stakeholders.

### Technical Goals
- Build an end-to-end analytics pipeline using the CMS Open Data API.
- Implement reliable data ingestion with pagination and basic error handling.
- Apply data cleaning and transformation steps to create analytics-ready datasets.
- Perform data quality checks, including missing values, range validation, and sanity checks.
- Separate exploratory analysis from production-style data processing.
- Produce reproducible and well-documented results suitable for a portfolio project.

-----

## Repository Structure
- data/
  - raw
  - clean
  - mart
  - README.md
- notebooks/
  - figures
- reports/
- src/
- README.md

-----