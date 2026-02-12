create or replace view mart_citations_year as
select *
from read_parquet('data/mart/mart_citations_year.parquet');

create or replace view mart_state_year as
select *
from read_parquet('data/mart/mart_state_year.parquet');

create or replace view mart_citations_month as
select *
from read_parquet('data/mart/mart_citations_month.parquet');