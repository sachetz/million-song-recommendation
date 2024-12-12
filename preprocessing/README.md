# Preprocessing

This script preprocesses the Million Song Dataset (MSD) by handling missing values, imputing critical features, and 
optimizing the dataset for downstream use in the recommendation engine.

Columns with missing values (artist_familiarity, artist_latitude, artist_longitude, song_hotttnesss) are identified.
artist_familiarity and song_hotttnesss are critical for the recommendation engine and are imputed using 
regression techniques.
Geolocation data is ignored due to its complexity and the lack of geolocation APIs in the scope of this project.
The artist_location column can potentially be used as a substitute but is not processed in this pipeline due to its 
complexity.

Non-essential columns that do not contribute to the recommendation engine (e.g., technical metadata, IDs, and redundant 
features) are removed.

The dataset is repartitioned into 20 partitions for better parallel processing and the preprocessed dataset is saved in 
Parquet format.