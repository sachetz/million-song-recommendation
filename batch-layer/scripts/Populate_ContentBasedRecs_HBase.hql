INSERT OVERWRITE TABLE sachetz_ContentBasedRecs_hbase
SELECT
    row_key,
    song_name,
    artist_name,
    album_name,
    year
FROM sachetz_ContentBasedRecs_hive;