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
year_totals as (
    select
        year,
        sum(citations_count) as year_citations_count,
        sum(total_fines_amount) as year_fines_amount
    from monthly
    group by year
)
select
    m.year,
    m.month,
    m.citations_count,
    m.total_fines_amount,
    round(m.total_fines_amount / nullif(m.citations_count, 0), 2) as avg_fine_amount,
    round(m.citations_count * 1.0 / nullif(y.year_citations_count, 0), 4) as share_of_year_citations,
    round(m.total_fines_amount * 1.0 / nullif(y.year_fines_amount, 0), 4) as share_of_year_fines
from monthly as m
join year_totals as y
  on m.year = y.year
order by m.year, m.month;