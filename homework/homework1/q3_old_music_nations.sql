-- List top 10 countries with the most classical music artists (born or started before 1850)
-- along with the number of associated artists. artist.area - area.id

SELECT area.name, COUNT(artist.id) AS cnt
FROM artist
INNER JOIN area ON artist.area = area.id
WHERE artist.begin_date_year < 1850
GROUP BY artist.area
ORDER BY cnt DESC
LIMIT 10;
