import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.Pipeline

object ALSBasedRecs {
    def main(args: Array[String]): Unit = {
        // Initialize Spark Session with Hive support
        val spark = SparkSession.builder()
            .appName("sachetz_batch_alsbasedrecs")
            .enableHiveSupport()
            .getOrCreate()

        import spark.implicits._

        // Load User Actions
        val userActions = spark.sql("SELECT user_id, song_id, rating FROM sachetz_user_actions")
            .filter($"rating".isNotNull && $"user_id".isNotNull && $"song_id".isNotNull)

        // Load MSD for Song Details
        val msd = spark.sql("SELECT song_id, title, artist_name, album_name, year FROM sachetz_msd")

        // Initialize StringIndexer for user_id
        val userIndexer = new StringIndexer()
            .setInputCol("user_id")
            .setOutputCol("user_id_indexed")
            .setHandleInvalid("skip") // Skip invalid or unseen labels

        // Initialize StringIndexer for song_id
        val songIndexer = new StringIndexer()
            .setInputCol("song_id")
            .setOutputCol("song_id_indexed")
            .setHandleInvalid("skip") // Skip invalid or unseen labels

        // Create Pipelines for indexing string ids to numeric values for ALS
        val indexerSongPipeline = new Pipeline().setStages(Array(songIndexer))
        val indexerActionPipeline = new Pipeline().setStages(Array(userIndexer))

        // Fit the Pipeline to the userActions and msd DataFrames
        val indexerSongModel = indexerSongPipeline.fit(msd)
        val indexerActionsModel = indexerActionPipeline.fit(userActions)

        // Transform the userActions dataframe with the two fit pipelines
        val songIndexedActions = indexerSongModel.transform(userActions)
        val userIndexedActions = indexerActionsModel.transform(songIndexedActions)
        val indexedUserActions = userIndexedActions
            .withColumn("rating_numeric", $"rating".cast("float")) // Cast rating to float for ALS
            .select("user_id_indexed", "song_id_indexed", "rating_numeric")
        val indexedMSD = indexerSongModel.transform(msd)

        // Configure ALS Model with Indexed Columns
        val als = new ALS()
            .setMaxIter(10)
            .setRegParam(0.1)
            .setUserCol("user_id_indexed")
            .setItemCol("song_id_indexed")
            .setRatingCol("rating_numeric")
            .setColdStartStrategy("drop") // To handle NaN predictions
            .setNonnegative(true) // Ensures latent factors are non-negative

        // Train ALS Model on Indexed Data
        val model = als.fit(indexedUserActions)

        // Generate Top 10 Recommendations per User
        val userRecs = model.recommendForAllUsers(10)

        // Explode Recommendations to have one recommendation per row
        val explodedRecs = userRecs
            .withColumn("rec", explode($"recommendations"))
            .select($"user_id_indexed", $"rec.song_id_indexed", $"rec.rating")

        // Join with indexed actions and msd tables to get the string ids from indexed ids
        val recsWithDetails = explodedRecs
            .join(userIndexedActions.select("user_id", "user_id_indexed").distinct(), Seq("user_id_indexed"), "left")
            .join(indexedMSD, Seq("song_id_indexed"))

        // Create row_key by concatenating user_id and song_id with a delimiter
        val hiveReadyRecs = recsWithDetails
            .withColumn("row_key", concat_ws("#", $"user_id", $"song_id"))
            .select(
                $"row_key",
                $"title".alias("song_name"),
                $"artist_name",
                $"album_name",
                $"year"
            )

        // Save Recommendations to Hive Table
        hiveReadyRecs.write
            .mode("overwrite")
            .insertInto("sachetz_ALSRecs_hive")

        // Stop Spark Session
        spark.stop()
    }
}