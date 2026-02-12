with agg as (
    select
        violation_description,
        sum(citations_count) as citations_count,
        sum(total_fines_amount) as total_fines_amount,
        sum(total_fines_amount) / nullif(sum(citations_count), 0) as avg_fine_amount
    from mart_citations_year
    group by violation_description
),
totals as (
    select
        sum(citations_count) as all_citations_count
    from agg
)
select
    a.violation_description,
    a.citations_count,
    a.total_fines_amount,
    a.avg_fine_amount,
    a.citations_count / nullif(t.all_citations_count, 0) as share_of_total_citations
from agg as a
cross join totals as t
order by a.citations_count desc
limit 10;