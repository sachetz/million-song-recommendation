CREATE EXTERNAL TABLE sachetz_msd_optimised (
    artist_hotttnesss DOUBLE,
    artist_name STRING,
    artist_terms ARRAY<STRING>,
    artist_terms_freq ARRAY<DOUBLE>,
    artist_terms_weight ARRAY<DOUBLE>,
    danceability DOUBLE,
    duration DOUBLE,
    energy DOUBLE,
    key INT,
    key_confidence DOUBLE,
    loudness DOUBLE,
    mode INT,
    mode_confidence DOUBLE,
    album_name STRING,
    segments_confidence ARRAY<DOUBLE>,
    segments_loudness_max ARRAY<DOUBLE>,
    segments_loudness_max_time ARRAY<DOUBLE>,
    segments_loudness_start ARRAY<DOUBLE>,
    segments_pitches ARRAY<ARRAY<DOUBLE>>,
    segments_start ARRAY<DOUBLE>,
    segments_timbre ARRAY<ARRAY<DOUBLE>>,
    similar_artists ARRAY<STRING>,
    song_id STRING,
    tatums_confidence ARRAY<DOUBLE>,
    tatums_start ARRAY<DOUBLE>,
    tempo DOUBLE,
    time_signature INT,
    time_signature_confidence DOUBLE,
    title STRING,
    track_id STRING,
    artist_familiarity DOUBLE,
    song_hotttnesss DOUBLE
)
PARTITIONED BY (year INT, artist_id STRING)
CLUSTERED BY (song_id) INTO 50 BUCKETS
STORED AS PARQUET
LOCATION "wasbs://hbase-mpcs5301-2024-10-20t23-28-51-804z@hbasempcs5301hdistorage.blob.core.windows.net/sachetz/msd_optimised"
TBLPROPERTIES ("parquet.compression"="SNAPPY")
;