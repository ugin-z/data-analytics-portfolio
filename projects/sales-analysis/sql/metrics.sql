SELECT
    date,
    country,
    SUM(revenue) AS daily_revenue,
    SUM(SUM(revenue)) OVER (
        PARTITION BY country
        ORDER BY date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rolling_7d_revenue
FROM sales
GROUP BY date, country;