with base as (
    select
        (case when vehicle_state = 'CA' then 'CA' else 'Non-CA' end) as state_group,
        sum(citations_count) as citations_count,
        sum(total_fines_amount) as total_fines_amount
    from mart_state_year
    where vehicle_state is not null
    group by state_group
),
totals as (
    select
        sum(citations_count) as all_citations_count,
        sum(total_fines_amount) as all_fines_amount
    from base
)
select
    b.state_group,
    b.citations_count,
    b.total_fines_amount,
    round(b.citations_count / nullif(t.all_citations_count, 0), 4) as share_of_total_citations,
    round(b.total_fines_amount / nullif(t.all_fines_amount, 0), 4) as share_of_total_fines
from base as b
cross join totals as t
order by b.state_group;