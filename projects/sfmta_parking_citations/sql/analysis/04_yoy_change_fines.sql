WITH base AS (
    SELECT
        violation_description,
        year,
        total_fines_amount
    FROM
        mart_citations_year
),
yoy AS (
    SELECT
        violation_description,
        year,
        total_fines_amount,
        lag(total_fines_amount) over (
            PARTITION by violation_description
            ORDER BY
                year
        ) AS prev_year_total_fines
    FROM
        base
),
scored AS (
    SELECT
        violation_description,
        year,
        total_fines_amount,
        prev_year_total_fines,
        (total_fines_amount - prev_year_total_fines) AS yoy_change_fines_amount,
        (total_fines_amount - prev_year_total_fines) * 1.0 / nullif(prev_year_total_fines, 0) AS yoy_change_fines_pct,
        row_number() over (
            PARTITION by year
            ORDER BY
                (total_fines_amount - prev_year_total_fines) DESC
        ) AS yoy_rank_in_year
    FROM
        yoy
    WHERE
        prev_year_total_fines IS NOT NULL
)
SELECT
    violation_description,
    year,
    total_fines_amount,
    prev_year_total_fines,
    yoy_change_fines_amount,
    round(yoy_change_fines_pct, 4) AS yoy_change_fines_pct,
    yoy_rank_in_year
FROM
    scored
WHERE
    yoy_rank_in_year <= 10
ORDER BY
    year DESC,
    yoy_rank_in_year;