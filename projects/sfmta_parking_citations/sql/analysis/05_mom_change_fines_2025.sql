WITH params AS (
    SELECT
        max(year) AS target_year
    FROM
        mart_citations_year_month
),
top10 AS (
    SELECT
        violation_description
    FROM
        (
            SELECT
                m.violation_description,
                sum(m.total_fines_amount) AS total_fines_amount_year,
                row_number() over (
                    ORDER BY
                        sum(m.total_fines_amount) DESC
                ) AS rnk
            FROM
                mart_citations_year_month m
                JOIN params p ON m.year = p.target_year
            GROUP BY
                m.violation_description
        )
    WHERE
        rnk <= 10
),
monthly AS (
    SELECT
        m.violation_description,
        m.year,
        m.month,
        m.total_fines_amount
    FROM
        mart_citations_year_month m
        JOIN params p ON m.year = p.target_year
    WHERE
        m.violation_description IN (
            SELECT
                violation_description
            FROM
                top10
        )
),
mom AS (
    SELECT
        violation_description,
        year,
        MONTH,
        total_fines_amount,
        lag(total_fines_amount) over (
            PARTITION by violation_description
            ORDER BY
                year,
                MONTH
        ) AS prev_month_total_fines
    FROM
        monthly
),
final AS (
    SELECT
        violation_description,
        year,
        MONTH,
        total_fines_amount,
        prev_month_total_fines,
        total_fines_amount - prev_month_total_fines AS mom_change_fines_amount,
        round(
            (total_fines_amount - prev_month_total_fines) * 1.0 / nullif(prev_month_total_fines, 0),
            4
        ) AS mom_change_fines_pct,
        row_number() over (
            PARTITION by year,
            MONTH
            ORDER BY
                total_fines_amount DESC
        ) AS mom_rank_in_month
    FROM
        mom
)
SELECT
    *
FROM
    final
WHERE
    prev_month_total_fines IS NOT NULL
ORDER BY
    violation_description,
    year,
    MONTH;