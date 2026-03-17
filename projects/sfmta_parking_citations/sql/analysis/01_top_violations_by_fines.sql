WITH agg AS (
    SELECT
        violation_description,
        sum(total_fines_amount) AS total_fines_amount,
        sum(citations_count) AS citations_count,
        sum(total_fines_amount) / nullif(sum(citations_count), 0) AS avg_fine_amount
    FROM
        mart_citations_year
    GROUP BY
        violation_description
),
totals AS (
    SELECT
        sum(total_fines_amount) AS all_fines_amount
    FROM
        agg
)
SELECT
    a.violation_description,
    a.total_fines_amount,
    a.citations_count,
    round(a.avg_fine_amount, 2),
    round(
        a.total_fines_amount / nullif(t.all_fines_amount, 0),
        4
    ) AS share_of_total_fines
FROM
    agg AS a
    CROSS JOIN totals AS t
ORDER BY
    a.total_fines_amount DESC
LIMIT
    10;