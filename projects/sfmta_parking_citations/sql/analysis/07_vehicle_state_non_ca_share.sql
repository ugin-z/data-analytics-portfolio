WITH base AS (
    SELECT
        (
            CASE
                WHEN vehicle_state = 'CA' THEN 'CA'
                ELSE 'Non-CA'
            END
        ) AS state_group,
        sum(citations_count) AS citations_count,
        sum(total_fines_amount) AS total_fines_amount
    FROM
        mart_state_year
    WHERE
        vehicle_state IS NOT NULL
    GROUP BY
        state_group
),
totals AS (
    SELECT
        sum(citations_count) AS all_citations_count,
        sum(total_fines_amount) AS all_fines_amount
    FROM
        base
)
SELECT
    b.state_group,
    b.citations_count,
    b.total_fines_amount,
    round(
        b.citations_count / nullif(t.all_citations_count, 0),
        4
    ) AS share_of_total_citations,
    round(
        b.total_fines_amount / nullif(t.all_fines_amount, 0),
        4
    ) AS share_of_total_fines
FROM
    base AS b
    CROSS JOIN totals AS t
ORDER BY
    b.state_group;