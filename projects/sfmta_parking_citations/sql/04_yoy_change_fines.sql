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
)
select
    violation_description,
    year,
    total_fines_amount,
    prev_year_total_fines,
    total_fines_amount - prev_year_total_fines as yoy_change_fines_amount,
    (total_fines_amount - prev_year_total_fines) / nullif(prev_year_total_fines, 0) as yoy_change_fines_pct
from yoy
where prev_year_total_fines is not null
order by year desc, yoy_change_fines_amount desc;