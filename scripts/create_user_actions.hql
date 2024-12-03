CREATE EXTERNAL TABLE sachetz_user_actions (
    song_id STRING,
    action_type STRING,
    action_time TIMESTAMP,
    rating DOUBLE
)
PARTITIONED BY (user_id STRING)
STORED AS PARQUET
LOCATION "wasbs://hbase-mpcs5301-2024-10-20t23-28-51-804z@hbasempcs5301hdistorage.blob.core.windows.net/sachetz/user_actions"
TBLPROPERTIES ("parquet.compression"="SNAPPY")
;