with agg as (
    select
        violation_description,
        sum(total_fines_amount) as total_fines_amount
    from mart_citations_year
    group by violation_description
),
ranked as (
    select
        violation_description,
        total_fines_amount,
        total_fines_amount / nullif(sum(total_fines_amount) over (), 0) as share_of_total_fines,
        sum(total_fines_amount) over (
            order by total_fines_amount desc
            rows between unbounded preceding and current row
        ) / nullif(sum(total_fines_amount) over (), 0) as cumulative_share_of_total_fines,
        row_number () over (order by total_fines_amount desc) as violation_rank
    from agg
)
select
    violation_rank,
    violation_description,
    total_fines_amount,
    share_of_total_fines,
    cumulative_share_of_total_fines,
    (cumulative_share_of_total_fines <= 0.80) as in_top_80_pct_bucket
from ranked
order by violation_rank;