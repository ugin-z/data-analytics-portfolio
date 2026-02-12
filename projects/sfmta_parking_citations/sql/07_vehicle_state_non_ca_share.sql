with base as (
    select
        vehicle_state,
        count(*) as citations_count
    from parking_citations_clean
    where vehicle_state is not null 
    group by vehicle_state
),
totals as (
    select
        sum(citations_count) as total_citations
    from base
)
select
    sum(case when vehicle_state <> 'CA' then citations_count else 0 end) as non_ca_citations,
    t.total_citations,
    sum(case when vehicle_state <> 'CA' then citations_count else 0 end) /
        nullif(t.total_citations, 0) as non_ca_share
from base
cross join totals as t;