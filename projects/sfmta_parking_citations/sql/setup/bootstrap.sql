CREATE
OR REPLACE VIEW mart_citations_year AS
SELECT
    *
FROM
    read_parquet('data/mart/mart_citations_year.parquet');

CREATE
OR REPLACE VIEW mart_citations_year_month AS
SELECT
    *
FROM
    read_parquet('data/mart/mart_citations_year_month.parquet');

CREATE
OR REPLACE VIEW mart_state_year AS
SELECT
    *
FROM
    read_parquet('data/mart/mart_state_year.parquet');

CREATE
OR REPLACE VIEW mart_citations_month AS
SELECT
    *
FROM
    read_parquet('data/mart/mart_citations_month.parquet');