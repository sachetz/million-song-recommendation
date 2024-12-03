import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.recommendation.ALS
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.util.Bytes
import com.hortonworks.hwc.HiveWarehouseSession
import com.hortonworks.spark.sql.hive.llap.HiveWarehouseSession.HIVE_WAREHOUSE_CONNECTOR

object ALSBasedRecs {
    def main(args: Array[String]): Unit = {
        // Initialize Spark Session with Hive support
        val spark = SparkSession.builder()
            .appName("sachetz_batch_alsbasedrecs")
            .getOrCreate()
        val hive = HiveWarehouseSession.session(spark).build()
        hive.setDatabase("default")

        val sachetz_user_actions = hive.table("sachetz_user_actions")
        sachetz_user_actions.createOrReplaceTempView("sachetz_user_actions")
        val sachetz_msd_optimised = hive.table("sachetz_msd_optimised")
        sachetz_msd_optimised.createOrReplaceTempView("sachetz_msd_optimised")

        import spark.implicits._

        // Load User Actions
        val userActions = spark.sql("SELECT user_id, song_id, rating FROM sachetz_user_actions")
            .filter($"rating".isNotNull && $"user_id".isNotNull && $"song_id".isNotNull)

        // Load MSD Optimised for Song Details
        val msd = spark.sql("SELECT song_id, title, artist_name, album_name, year FROM sachetz_msd_optimised")
            .select("song_id", "title", "artist_name", "album_name", "year")

        // Prepare ALS Model
        val als = new ALS()
            .setMaxIter(10)
            .setRegParam(0.1)
            .setUserCol("user_id")
            .setItemCol("song_id")
            .setRatingCol("rating")
            .setColdStartStrategy("drop") // To handle NaN predictions

        // Train ALS Model
        val model = als.fit(userActions)

        // Generate Top 10 Recommendations per User
        val userRecs = model.recommendForAllUsers(10)

        // Explode Recommendations and Join with MSD for Details
        val explodedRecs = userRecs
            .withColumn("rec", explode($"recommendations"))
            .select($"user_id", $"rec.song_id", $"rec.rating".alias("predicted_rating"))

        val recsWithDetails = explodedRecs.join(msd, "song_id")

        val hiveReadyRecs = recsWithDetails
            .withColumn("row_key", concat_ws("#", $"user_id", $"song_id")) // Create row_key by combining user_id and song_id
            .select(
                $"row_key",
                $"title".alias("song_name"),
                $"artist_name",
                $"album_name",
                $"year"
            )

        // Save Recommendations to Hive Table
        hiveReadyRecs.write
            .format(HIVE_WAREHOUSE_CONNECTOR)
            .mode("overwrite")
            .option("table", "sachetz_ALSRecs_hive")
            .save()

        // Stop Spark Session
        spark.stop()
    }
}