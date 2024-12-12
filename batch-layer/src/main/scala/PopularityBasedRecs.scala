import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object PopularityBasedRecs {
    def main(args: Array[String]): Unit = {
        // Initialize Spark Session with Hive support
        val spark = SparkSession.builder()
            .appName("sachetz_batch_popularitybasedrecs")
            .enableHiveSupport()
            .getOrCreate()

        import spark.implicits._

        // Load MSD Table
        val msd = spark.sql("SELECT song_id, title, artist_name, album_name, year, song_hotttnesss, artist_hotttnesss FROM sachetz_msd")
            .filter($"song_hotttnesss".isNotNull && $"artist_hotttnesss".isNotNull)

        // Compute Popularity Score
        val popularityWeightSong = 0.7
        val popularityWeightArtist = 0.3
        val popularSongs = msd.withColumn("popularity_score",
            $"song_hotttnesss" * popularityWeightSong + $"artist_hotttnesss" * popularityWeightArtist
        )

        // Select Top N Popular Songs
        val topN = 100
        val topPopularSongs = popularSongs.orderBy(desc("popularity_score"))
            .limit(topN)
            .withColumnRenamed("song_id", "row_key")
            .withColumnRenamed("title", "song_name")
            .select("row_key", "song_name", "artist_name", "album_name", "year")

        // Write the DataFrame to the Hive table
        topPopularSongs.write
            .mode("overwrite")
            .insertInto("sachetz_PopularRecs_hive")

        // Stop Spark Session
        spark.stop()
    }
}