WITH agg AS (
    SELECT
        violation_description,
        sum(total_fines_amount) AS total_fines_amount
    FROM
        mart_citations_year
    GROUP BY
        violation_description
),
ranked AS (
    SELECT
        violation_description,
        total_fines_amount,
        total_fines_amount / nullif(sum(total_fines_amount) over (), 0) AS share_of_total_fines,
        sum(total_fines_amount) over (
            ORDER BY
                total_fines_amount DESC ROWS BETWEEN unbounded preceding
                AND current ROW
        ) / nullif(sum(total_fines_amount) over (), 0) AS cumulative_share_of_total_fines,
        row_number () over (
            ORDER BY
                total_fines_amount DESC
        ) AS violation_rank
    FROM
        agg
)
SELECT
    violation_rank,
    violation_description,
    total_fines_amount,
    round(share_of_total_fines, 4),
    round(cumulative_share_of_total_fines, 4),
    (cumulative_share_of_total_fines <= 0.80) AS in_top_80_pct_bucket
FROM
    ranked
ORDER BY
    violation_rank;