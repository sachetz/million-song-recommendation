INSERT OVERWRITE TABLE sachetz_PopularRecs_hbase
SELECT
    row_key,
    song_name,
    artist_name,
    album_name,
    year
FROM sachetz_PopularRecs_hive;