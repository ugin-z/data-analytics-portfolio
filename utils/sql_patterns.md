## Pattern - Channel efficiency (CPA)

```sql
SELECT
    channel,
    SUM(spend) AS total_spend,
    SUM(conversions) AS total_conversions,
    SUM(spend) / NULLIF(SUM(conversions), 0) AS cpa
FROM mart_ads_day
GROUP BY channel;
