CREATE EXTERNAL TABLE sachetz_ContentBasedRecs_hbase (
    row_key STRING,
    song_name STRING,
    artist_name STRING,
    album_name STRING,
    year STRING
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
    'hbase.columns.mapping' = ':key,details:song_name#b,details:artist_name#b,details:album_name#b,details:year#b'
)
TBLPROPERTIES (
    'hbase.table.name' = 'sachetz_ContentBasedRecs'
);

CREATE EXTERNAL TABLE sachetz_ContentBasedRecs_hive (
    row_key STRING,
    song_name STRING,
    artist_name STRING,
    album_name STRING,
    year STRING
)
STORED AS ORC
LOCATION "wasbs://hbase-mpcs5301-2024-10-20t23-28-51-804z@hbasempcs5301hdistorage.blob.core.windows.net/sachetz/ContentBasedRecs_hive";



CREATE EXTERNAL TABLE sachetz_ALSRecs_hbase (
    row_key STRING,
    song_name STRING,
    artist_name STRING,
    album_name STRING,
    year STRING
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
    'hbase.columns.mapping' = ':key,details:song_name#b,details:artist_name#b,details:album_name#b,details:year#b'
)
TBLPROPERTIES (
    'hbase.table.name' = 'sachetz_ALSRecs'
);

CREATE EXTERNAL TABLE sachetz_ALSRecs_hive (
    row_key STRING,
    song_name STRING,
    artist_name STRING,
    album_name STRING,
    year STRING
)
STORED AS ORC
LOCATION "wasbs://hbase-mpcs5301-2024-10-20t23-28-51-804z@hbasempcs5301hdistorage.blob.core.windows.net/sachetz/ALSRecs_hive";



CREATE EXTERNAL TABLE sachetz_PopularRecs_hbase (
    row_key STRING,
    song_name STRING,
    artist_name STRING,
    album_name STRING,
    year STRING
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
    'hbase.columns.mapping' = ':key,details:song_name#b,details:artist_name#b,details:album_name#b,details:year#b'
)
TBLPROPERTIES (
    'hbase.table.name' = 'sachetz_PopularRecs'
);

CREATE EXTERNAL TABLE sachetz_PopularRecs_hive (
    row_key STRING,
    song_name STRING,
    artist_name STRING,
    album_name STRING,
    year STRING
)
STORED AS ORC
LOCATION "wasbs://hbase-mpcs5301-2024-10-20t23-28-51-804z@hbasempcs5301hdistorage.blob.core.windows.net/sachetz/PopularRecs_hive";



CREATE EXTERNAL TABLE sachetz_OngoingRecs_hbase (
    row_key STRING,
    song_name STRING,
    artist_name STRING,
    album_name STRING,
    year STRING
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
    'hbase.columns.mapping' = ':key,details:song_name#b,details:artist_name#b,details:album_name#b,details:year#b'
)
TBLPROPERTIES (
    'hbase.table.name' = 'sachetz_OngoingRecs'
);

CREATE EXTERNAL TABLE sachetz_OngoingRecs_hive (
    row_key STRING,
    song_name STRING,
    artist_name STRING,
    album_name STRING,
    year STRING
)
STORED AS ORC
LOCATION "wasbs://hbase-mpcs5301-2024-10-20t23-28-51-804z@hbasempcs5301hdistorage.blob.core.windows.net/sachetz/OngoingRecs_hive";
