-- For each work type, find works that have the longest names. name - type
-- order it according to work type (ascending) and use work name (ascending) as tie-breaker.
SELECT w.name, t.name
FROM work AS w
    INNER JOIN (
            SELECT 
                MAX(LENGTH(work.name)) AS max_length,
                work.type AS type
            FROM work
            GROUP BY work.type
        ) AS info
        ON LENGTH(w.name) = info.max_length
        AND w.type = info.type
    INNER JOIN work_type AS t
        ON w.type = t.id
ORDER BY w.type, w.name;
