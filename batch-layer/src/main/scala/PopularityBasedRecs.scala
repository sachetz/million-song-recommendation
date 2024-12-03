import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
//import com.hortonworks.hwc.HiveWarehouseSession
//import com.hortonworks.spark.sql.hive.llap.HiveWarehouseSession.HIVE_WAREHOUSE_CONNECTOR

object PopularityBasedRecs {
    def main(args: Array[String]): Unit = {
        // Initialize Spark Session with Hive support
        val spark = SparkSession.builder()
            .appName("sachetz_batch_popularitybasedrecs")
            .enableHiveSupport()
            .getOrCreate()
//        val hive = HiveWarehouseSession.session(spark).build()
//        hive.setDatabase("default")
//
//        val sachetz_user_actions = hive.table("sachetz_user_actions")
//        sachetz_user_actions.createOrReplaceTempView("sachetz_user_actions")
//        val sachetz_msd_optimised = hive.table("sachetz_msd")
//        sachetz_msd_optimised.createOrReplaceTempView("sachetz_msd_optimised")

        import spark.implicits._

        // Load MSD Optimised Table
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
            .withColumnRenamed("song_id", "row_key") // Rename to match the external table schema
            .withColumnRenamed("title", "song_name") // Rename to match the external table schema
            .select("row_key", "song_name", "artist_name", "album_name", "year") // Select only required columns

        // Write the DataFrame to the external Hive table location
        topPopularSongs.write
            .mode("overwrite")
            .insertInto("sachetz_PopularRecs_hive")

        // Stop Spark Session
//        hive.close()
        spark.stop()
    }
}