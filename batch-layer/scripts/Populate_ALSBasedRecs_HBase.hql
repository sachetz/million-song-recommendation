INSERT OVERWRITE TABLE sachetz_ALSRecs_hbase
SELECT
    row_key,
    song_name,
    artist_name,
    album_name,
    year
FROM sachetz_ALSRecs_hive;