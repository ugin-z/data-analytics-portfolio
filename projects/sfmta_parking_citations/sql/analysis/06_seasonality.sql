WITH monthly AS (
    SELECT
        year,
        MONTH,
        sum(citations_count) AS citations_count,
        sum(total_fines_amount) AS total_fines_amount
    FROM
        mart_citations_month
    WHERE
        year IS NOT NULL
        AND MONTH IS NOT NULL
    GROUP BY
        year,
        MONTH
),
year_totals AS (
    SELECT
        year,
        sum(citations_count) AS year_citations_count,
        sum(total_fines_amount) AS year_fines_amount
    FROM
        monthly
    GROUP BY
        year
)
SELECT
    m.year,
    m.month,
    m.citations_count,
    m.total_fines_amount,
    round(
        m.total_fines_amount / nullif(m.citations_count, 0),
        2
    ) AS avg_fine_amount,
    round(
        m.citations_count * 1.0 / nullif(y.year_citations_count, 0),
        4
    ) AS share_of_year_citations,
    round(
        m.total_fines_amount * 1.0 / nullif(y.year_fines_amount, 0),
        4
    ) AS share_of_year_fines
FROM
    monthly AS m
    JOIN year_totals AS y ON m.year = y.year
ORDER BY
    m.year,
    m.month;