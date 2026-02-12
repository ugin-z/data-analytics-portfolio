with monthly as (
    select
        year,
        month,
        sum(citations_count) as citations_count,
        sum(total_fines_amount) as total_fines_amount
    from mart_citations_month
    where year is not null 
        and month is not null
    group by year, month
),
month_rollup as (
    select
        month,
        sum(total_fines_amount) as total_fines_amount,
        sum(citations_count) as citations_count,
        sum(total_fines_amount) / nullif(sum(citations_count), 0) as avg_fine_amount
    from monthly
    group by month
),
totals as (
    select
        sum(total_fines_amount) as all_fines_amount
    from month_rollup
)
select
    m.month,
    m.total_fines_amount,
    m.citations_count,
    m.avg_fine_amount,
    m.total_fines_amount / nullif(t.all_fines_amount, 0) as share_of_total_fines
from month_rollup as m
cross join totals as t
order by m.month;