-- List the month and the percentage of all releases issued in the corresponding
-- month all over the world in the past year. Display like 2020.01|5.95.

-- Details: The percentage of releases for a month is the number of releases issued in that
-- month devided by the total releases in the past year from 07/2019 to 07/2020, both included.
-- Releases with different issue dates or countries are considered different releases.
-- Round the percentage to two decimal places using ROUND(). Sort by dates in ascending order.
WITH past_year_release(year, month, cnt) AS (
    SELECT date_year, date_month, COUNT(release)
    FROM   release_info
    WHERE (date_year = 2019 AND date_month >= 7)
    OR    (date_year = 2020 AND date_month <= 7)
    GROUP BY date_year * 100 + date_month
)
SELECT CAST(year AS VARCHAR) || '.' || (
    CASE
        WHEN month < 10 then '0' || CAST(month AS VARCHAR)
        ELSE CAST(month AS VARCHAR)
    END
) AS date, ROUND(cnt * 100.0 / (SELECT SUM(cnt) FROM past_year_release), 2)
FROM past_year_release
ORDER BY date;
