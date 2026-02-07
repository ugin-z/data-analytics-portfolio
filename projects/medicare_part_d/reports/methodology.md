# Methodology

- Data source: public CMS Medicare Part D data (2023).
- Data architecture: raw → clean → mart.
- Three analytical data marts were created:
  - mart_drug_year
  - mart_prescriber_drug_year
  - mart_prescriber_year
- All analytics were performed using SQL via DuckDB.
- Pandas was used only for data preparation and mart construction.
- Results focus on business interpretation rather than descriptive statistics.