-- List the top 10 dubbed artist names with the number of dubs.
-- Print the
-- 1. artist name in the artist table
-- 2. the number of corresponding distinct dubbed artist names in the artist_alias table.
-- Count the number of distinct names in artist_alias for each artist in the artist table
-- 1. and list only the top ten
-- 2. who's from the United Kingdom and started after 1950 (not included)

SELECT artist.name, COUNT(artist_alias.id) AS cnt
FROM artist
INNER JOIN artist_alias ON artist.id = artist_alias.artist
WHERE artist.begin_date_year > 1950
AND artist.area = (SELECT id FROM area WHERE name = 'United Kingdom')
GROUP BY artist.id
ORDER BY cnt DESC
LIMIT 10;
