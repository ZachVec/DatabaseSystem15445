-- Concat all dubbed names of The Beatles using comma-separated values
-- Like "Beetles, fab four".
-- Details:
-- 1. Find all dubbed names of artist "The Beatles" in artist_alias
-- 2. and order them by id (ASC).
-- Print a single string containing all the dubbed names separated by commas.

-- Hint: You might find CTEs useful.
WITH dubs(seqno, name) AS (
    SELECT ROW_NUMBER() OVER(ORDER BY id), name
    FROM artist_alias
    WHERE artist = (SELECT id FROM artist WHERE name = 'The Beatles')
), flattened(seqno, name) AS (
    SELECT seqno, name
    FROM dubs
    WHERE seqno = 1

    UNION ALL

    SELECT dubs.seqno, flattened.name || ', ' || dubs.name
    FROM dubs
        INNER JOIN flattened ON flattened.seqno + 1 = dubs.seqno
)
SELECT name
FROM flattened
ORDER BY seqno DESC
LIMIT 1;
