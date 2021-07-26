-- Requirements: List rank, names, and the number of collaborative release of
--  Dr.Dre and Eminem among other most productive duos.
-- 1. Only releases in English are considered.
-- 2. All pairs of names should have the alphabetically smaller one first, or would be counted twice.
-- 3. Both artists should be solo artists, i.e., 1|Person in table artist_type
-- 4. Both started after 1960 (not included).
-- Display like: [rank]|Dr. Dre|Eminem|[# of releases].

WITH duos(id1, id2, cnt) AS (
    SELECT credit1.artist, credit2.artist, COUNT(*)
    FROM release as rel
        INNER JOIN artist_credit_name AS credit1 ON credit1.artist_credit = rel.artist_credit
        INNER JOIN artist_credit_name AS credit2 ON credit2.artist_credit = rel.artist_credit
        INNER JOIN artist AS artist1 ON credit1.artist = artist1.id
        INNER JOIN artist AS artist2 ON credit2.artist = artist2.id
    WHERE rel.language = (SELECT id FROM language WHERE name='English')
        AND credit1.name < credit2.name
        AND artist1.type = (SELECT id FROM artist_type WHERE name = 'Person')
        AND artist2.type = (SELECT id FROM artist_type WHERE name = 'Person')
        AND artist1.begin_date_year > 1960
        AND artist2.begin_date_year > 1960
    GROUP BY credit1.artist, credit2.artist
), result(rank, name1, name2, cnt) AS (
    SELECT ROW_NUMBER() OVER(
        ORDER BY duos.cnt DESC, artist1.name, artist2.name
    ), artist1.name, artist2.name, cnt
    FROM duos
        INNER JOIN artist AS artist1 ON duos.id1 = artist1.id
        INNER JOIN artist AS artist2 ON duos.id2 = artist2.id
)
SELECT *
FROM result
WHERE name1 = 'Dr. Dre'
AND name2 = 'Eminem';
