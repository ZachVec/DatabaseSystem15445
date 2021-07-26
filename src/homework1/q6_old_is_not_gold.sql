-- Which decades saw the most number of official releases?
-- List the number of official releases in every decade since 1900. Like 1970s|57210.
-- Details: 
-- Print all decades and the number of official releases.
-- Releases with different issue dates or countries are considered different releases.
-- Print the relevant decade in a fancier format by constructing a string
-- that looks like this: 1970s. Sort the decades in decreasing order with
-- respect to the number of official releases and use decade (descending) as tie-breaker.
-- Remember to exclude releases whose dates are NULL.
WITH info(decade) AS (
    SELECT (CAST((release_info.date_year / 10) AS INT) * 10) || 's'
    FROM release
        INNER JOIN release_info ON release.id = release_info.release
    WHERE release_info.date_year >= 1900
    AND release.status IN (SELECT id FROM release_status WHERE name = 'Official')
)
SELECT decade, COUNT(*) AS cnt
FROM info
GROUP BY decade
ORDER BY cnt DESC, decade DESC;
