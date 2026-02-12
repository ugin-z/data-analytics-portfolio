with params as (
    select max(year) as target_year
    from mart_citations_year_month
),
top10 as (
    select violation_description
    from (
        select
            m.violation_description,
            sum(m.total_fines_amount) as total_fines_amount_year,
            row_number() over (
                order by sum(m.total_fines_amount) desc
            ) as rnk
        from mart_citations_year_month m
        join params p on m.year = p.target_year
        group by m.violation_description
    )
    where rnk <= 10
),
monthly as (
    select
        m.violation_description,
        m.year,
        m.month,
        m.total_fines_amount
    from mart_citations_year_month m
    join params p on m.year = p.target_year
    where m.violation_description in (select violation_description from top10)
),
mom as (
    select
        violation_description,
        year,
        month,
        total_fines_amount,
        lag(total_fines_amount) over (
            partition by violation_description
            order by year, month
        ) as prev_month_total_fines
    from monthly
),
final as (
    select
        violation_description,
        year,
        month,
        total_fines_amount,
        prev_month_total_fines,
        total_fines_amount - prev_month_total_fines as mom_change_fines_amount,
        round(
            (total_fines_amount - prev_month_total_fines) * 1.0 / nullif(prev_month_total_fines, 0),
            4
        ) as mom_change_fines_pct,

        row_number() over (
            partition by year, month
            order by total_fines_amount desc
        ) as mom_rank_in_month
    from mom
)
select *
from final
where prev_month_total_fines is not null
order by violation_description, year, month;
