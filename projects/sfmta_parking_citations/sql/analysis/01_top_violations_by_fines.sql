with agg as (
    select
        violation_description,
        sum(total_fines_amount) as total_fines_amount,
        sum(citations_count) as citations_count,
        sum(total_fines_amount) / nullif(sum(citations_count), 0) as avg_fine_amount
    from mart_citations_year
    group by violation_description
),
totals as (
    select
        sum(total_fines_amount) as all_fines_amount
    from agg
)
select
    a.violation_description,
    a.total_fines_amount,
    a.citations_count,
    round(a.avg_fine_amount, 2),
    round(a.total_fines_amount / nullif(t.all_fines_amount, 0), 4) as share_of_total_fines
from agg as a
cross join totals as t
order by a.total_fines_amount desc
limit 10;