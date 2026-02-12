with base as (
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
        sum(citations_count) as citations_count,
        sum(total_fines_amount) as total_fines_amount,
        sum(total_fines_amount) / nullif(sum(citations_count), 0) as avg_fine_amount
    from base
    group by month
),
totals as (
    select
        sum(citations_count) as all_citations_count
    from month_rollup
)
select
    m.month,
    m.citations_count,
    m.total_fines_amount,
    m.avg_fine_amount,
    m.citations_count / nullif(t.all_citations_count, 0) as share_of_total_citations
from month_rollup as m
cross join totals as t
order by m.month;