# SQL Patterns Used in This Portfolio

This document describes SQL patterns that are actively used across this repository
for analytics engineering workflows: clean → mart layers, reproducibility, and DQ.

1. Explicit grain in marts
   -- Always define grain in SELECT and GROUP BY
   SELECT
       prescriber_npi,
       drug_name,
       year,
       SUM(total_drug_cost) AS total_drug_cost,
       COUNT(*) AS claim_count
   FROM clean_claims
   GROUP BY prescriber_npi, drug_name, year;

2. Deterministic aggregations (no SELECT *)
   -- Avoid SELECT * in marts to keep schema stable
   SELECT
       prescriber_npi,
       year,
       SUM(total_drug_cost) AS total_drug_cost
   FROM clean_claims
   GROUP BY prescriber_npi, year;

3. Defensive division with NULLIF
   -- Prevent division by zero
   SELECT
       drug_name,
       SUM(total_drug_cost) / NULLIF(SUM(claim_count), 0) AS avg_cost_per_claim
   FROM mart_drug_year
   GROUP BY drug_name;

4. Window functions for ranking (Top-N)
   -- Rank within a partition (used for Top-N dashboards)
   SELECT
       year,
       drug_name,
       total_drug_cost,
       RANK() OVER (PARTITION BY year ORDER BY total_drug_cost DESC) AS cost_rank
   FROM mart_drug_year;

5. De-duplication using ROW_NUMBER
   -- Keep the latest record per business key
   SELECT *
   FROM (
       SELECT
           *,
           ROW_NUMBER() OVER (
               PARTITION BY prescriber_npi, drug_name, year
               ORDER BY updated_at DESC
           ) AS rn
       FROM clean_claims
   ) t
   WHERE rn = 1;

6. Consistent filtering via CTEs
   -- Apply filters once and reuse downstream
   WITH base AS (
       SELECT *
       FROM clean_claims
       WHERE total_drug_cost >= 0
   )
   SELECT
       prescriber_npi,
       year,
       SUM(total_drug_cost) AS total_drug_cost
   FROM base
   GROUP BY prescriber_npi, year;

7. Explicit type casting
   -- Ensure numeric stability
   SELECT
       CAST(year AS INTEGER) AS year,
       CAST(total_drug_cost AS DOUBLE) AS total_drug_cost
   FROM clean_claims;

8. DuckDB-friendly syntax
   -- Used when loading marts into DuckDB
   CREATE OR REPLACE TABLE mart_prescriber_year AS
   SELECT
       prescriber_npi,
       year,
       SUM(total_drug_cost) AS total_drug_cost
   FROM clean_claims
   GROUP BY prescriber_npi, year;

