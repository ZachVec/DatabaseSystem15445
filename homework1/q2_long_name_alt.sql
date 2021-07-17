-- For each work type, find works that have the longest names. name - type
-- order it according to work type (ascending) and use work name (ascending) as tie-breaker.
WITH info(max_length, type) AS (
    SELECT MAX(LENGTH(name)), type
    FROM work
    GROUP BY type
)
SELECT work.name, work_type.name
FROM work
    INNER JOIN info
        ON work.type = info.type
        AND LENGTH(work.name) = info.max_length
    INNER JOIN work_type
        ON work.type = work_type.id
ORDER BY work.type, work.name;
