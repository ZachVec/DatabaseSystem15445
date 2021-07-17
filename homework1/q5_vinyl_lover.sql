-- List the distinct names of releases
-- 1. issued in vinyl format(Vinyl format includes ALL vinyl dimensions excluding VinylDisc.)
-- 2. by the British band Coldplay.
-- 3. Sort the release names by release date ascendingly.

SELECT DISTINCT(release.name)
FROM release
    INNER JOIN medium ON medium.release = release.id
    INNER JOIN medium_format ON medium.format = medium_format.id
    INNER JOIN artist_credit ON artist_credit.id = release.artist_credit
    INNER JOIN release_info  ON release_info.release = release.id
WHERE medium_format.name LIKE '%Vinyl'
    AND artist_credit.name = 'Coldplay'
ORDER BY release_info.date_year, release_info.date_month, release_info.date_day;
