WITH agg AS (
    SELECT
        violation_description,
        sum(citations_count) AS citations_count,
        sum(total_fines_amount) AS total_fines_amount,
        sum(total_fines_amount) / nullif(sum(citations_count), 0) AS avg_fine_amount
    FROM
        mart_citations_year
    GROUP BY
        violation_description
),
totals AS (
    SELECT
        sum(citations_count) AS all_citations_count
    FROM
        agg
)
SELECT
    a.violation_description,
    a.citations_count,
    a.total_fines_amount,
    round(a.avg_fine_amount, 2),
    round(
        a.citations_count / nullif(t.all_citations_count, 0),
        4
    ) AS share_of_total_citations
FROM
    agg AS a
    CROSS JOIN totals AS t
ORDER BY
    a.citations_count DESC
LIMIT
    10;