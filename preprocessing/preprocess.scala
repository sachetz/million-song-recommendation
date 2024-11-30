val raw_msd_df = spark.read.parquet("/sachetz/msd_a")

raw_msd_df.columns.filter(c => raw_msd_df.filter(col(c).isNull).count > 0)
// Empty Columns: Array(artist_familiarity, artist_latitude, artist_longitude, song_hotttnesss)
// artist_familiarity and song_hotttnesss might be important columns in the recommendation
// artist_location might help substitute for artist_latitude and artist_longitude

// Using linear regression to approximate artist_familiarity and song_hotttnesss
import org.apache.spark.ml.feature.{VectorAssembler, Imputer}
import org.apache.spark.ml.regression.LinearRegression

// artist_familiarity
val featureColsArtistFamiliarity = Array("artist_hotttnesss", "year", "danceability", "energy", "loudness", "tempo")
val assemblerArtistFamiliarity = new VectorAssembler().setInputCols(featureColsArtistFamiliarity).setOutputCol("artist_familiarity_features")
val assembledArtistFamiliarityDF = assemblerArtistFamiliarity.transform(raw_msd_df)
val lraf = new LinearRegression().setLabelCol("artist_familiarity").setFeaturesCol("artist_familiarity_features")
val modelaf = lraf.fit(assembledArtistFamiliarityDF.na.drop())
val predictionsArtistFamiliarity = modelaf.transform(assembledArtistFamiliarityDF)
val filledArtistFamiliarityDF = predictionsArtistFamiliarity.withColumn("artist_familiarity_filled",
  when(col("artist_familiarity").isNull, col("prediction")).otherwise(col("artist_familiarity"))
).drop("prediction")
filledArtistFamiliarityDF.show()

// song_hotttnesss
val featureColsSongHotness = Array("artist_hotttnesss", "year", "danceability", "energy", "loudness", "tempo")
val assemblerSongHotness = new VectorAssembler().setInputCols(featureColsSongHotness).setOutputCol("song_hottness_feature")
val assembledSongHotnessDF = assemblerSongHotness.transform(filledArtistFamiliarityDF)
val lrsh = new LinearRegression().setLabelCol("song_hotttnesss").setFeaturesCol("song_hottness_feature")
val modelsh = lrsh.fit(assembledSongHotnessDF.na.drop())
val predictionsSongHotness = modelsh.transform(assembledSongHotnessDF)
val filledSongHotnessDF = predictionsSongHotness.withColumn("song_hotttnesss_filled",
    when(col("song_hotttnesss").isNull, col("prediction")).otherwise(col("song_hotttnesss"))
).drop("prediction")
filledSongHotnessDF.show()

// Processing artist_latitude and artist_longitude, and handling the variability of the data in
// artist_location requires advanced techniques and the use of geolocation APIs, which is out of
// scope of this project, so I will be ignoring those columns.

// Dropping columns not useful in building the recommendation engine
// Using the Echo Nest metadata for the songs
val preprocessedDF = filledSongHotnessDF.drop(
        "analysis_sample_rate",
        "artist_7digitalid",
        "artist_familiarity",
        "artist_latitude",
        "artist_location",
        "artist_longitude",
        "artist_mbid",
        "artist_mbtags",
        "artist_mbtags_count",
        "artist_playmeid",
        "audio_md5",
        "bars_confidence",
        "bars_start",
        "beats_confidence",
        "beats_start",
        "end_of_fade_in",
        "release_7digitalid",
        "sections_confidence",
        "sections_start",
        "song_hotttnesss",
        "start_of_fade_out",
        "track_7digitalid",
        "song_hottness_feature",
        "artist_familiarity_features"
    ).withColumnRenamed("artist_familiarity_filled", "artist_familiarity").withColumnRenamed("song_hotttnesss_filled", "song_hotttnesss").withColumnRenamed("release", "album_name")

val optimizedDF = preprocessedDF.repartition(20)
optimizedDF.write.mode("overwrite").parquet("/sachetz/msd")
