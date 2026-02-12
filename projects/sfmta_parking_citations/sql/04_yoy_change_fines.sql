with base as (
    select
        violation_description,
        year,
        total_fines_amount
    from mart_citations_year
),
yoy as (
    select
        violation_description,
        year,
        total_fines_amount,
        lag(total_fines_amount) over (
            partition by violation_description
            order by year
        ) as prev_year_total_fines
    from base
),
scored as (
    select
        violation_description,
        year,
        total_fines_amount,
        prev_year_total_fines,
        (total_fines_amount - prev_year_total_fines) as yoy_change_fines_amount,
        (total_fines_amount - prev_year_total_fines) * 1.0 / nullif(prev_year_total_fines, 0) as yoy_change_fines_pct,
        row_number() over (
            partition by year
            order by (total_fines_amount - prev_year_total_fines) desc
        ) as yoy_rank_in_year
    from yoy
    where prev_year_total_fines is not null
)
select
    violation_description,
    year,
    total_fines_amount,
    prev_year_total_fines,
    yoy_change_fines_amount,
    round(yoy_change_fines_pct, 4) as yoy_change_fines_pct,
    yoy_rank_in_year
from scored
where yoy_rank_in_year <= 10
order by year desc, yoy_rank_in_year;