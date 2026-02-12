with top10 as (
    select violation_description
    from (
        select
            violation_description,
            sum(total_fines_amount) as total_fines_amount,
            row_number() over (
                order by sum(total_fines_amount) desc
            ) as rnk
        from mart_citations_year_month
        group by violation_description
    )
    where rnk <= 10
),
monthly as (
    select
        violation_description,
        year,
        month,
        total_fines_amount
    from mart_citations_year_month
    where year = (select max(year) from mart_citations_month)
      and violation_description in (select violation_description from top10)
),
mom as (
    select
        violation_description,
        month,
        total_fines_amount,
        lag(total_fines_amount) over (
            partition by violation_description
            order by month
        ) as prev_month_fines
    from monthly
)
select
    violation_description,
    month,
    total_fines_amount,
    total_fines_amount - prev_month_fines as mom_change_amount,
    round((total_fines_amount - prev_month_fines) * 1.0 / nullif(prev_month_fines, 0), 4) as mom_change_pct
from mom
where prev_month_fines is not null
order by violation_description, month;