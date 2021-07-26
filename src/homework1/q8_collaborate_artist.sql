-- List the number of artists who have collaborated with Ariana Grande.
-- Details:
-- Print only the total number of artists. An artist is considered a collaborator
-- if they appear in the same artist_credit with Ariana Grande.
-- The answer should include Ariana Grande herself.
WITH collaboration(credit) AS (
    SELECT artist_credit
    FROM artist_credit_name
    WHERE name = 'Ariana Grande'
)
SELECT COUNT(DISTINCT(artist))
FROM artist_credit_name
WHERE artist_credit IN collaboration;
