CREATE EXTERNAL TABLE sachetz_msd_optimised (
    artist_hotttnesss FLOAT,
    artist_name STRING,
    artist_terms ARRAY<STRING>,
    artist_terms_freq ARRAY<FLOAT>,
    artist_terms_weight ARRAY<FLOAT>,
    danceability FLOAT,
    duration FLOAT,
    energy FLOAT,
    key INT,
    key_confidence FLOAT,
    loudness FLOAT,
    mode INT,
    mode_confidence FLOAT,
    album_name STRING,
    segments_confidence ARRAY<FLOAT>,
    segments_loudness_max ARRAY<FLOAT>,
    segments_loudness_max_time ARRAY<FLOAT>,
    segments_loudness_start ARRAY<FLOAT>,
    segments_pitches ARRAY<ARRAY<FLOAT>>,
    segments_start ARRAY<FLOAT>,
    segments_timbre ARRAY<ARRAY<FLOAT>>,
    similar_artists ARRAY<STRING>,
    song_id STRING,
    tatums_confidence ARRAY<FLOAT>,
    tatums_start ARRAY<FLOAT>,
    tempo FLOAT,
    time_signature INT,
    time_signature_confidence FLOAT,
    title STRING,
    track_id STRING,
    artist_familiarity FLOAT,
    song_hotttnesss FLOAT
)
PARTITIONED BY (year INT, artist_id STRING)
CLUSTERED BY (song_id) INTO 50 BUCKETS
STORED AS PARQUET
LOCATION "wasbs://hbase-mpcs5301-2024-10-20t23-28-51-804z@hbasempcs5301hdistorage.blob.core.windows.net/sachetz/msd_optimised"
TBLPROPERTIES ("parquet.compression"="SNAPPY")
;