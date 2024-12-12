import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.mllib.linalg
//import com.hortonworks.hwc.HiveWarehouseSession
//import com.hortonworks.spark.sql.hive.llap.HiveWarehouseSession.HIVE_WAREHOUSE_CONNECTOR

object ContentBasedRecs {
    def main(args: Array[String]): Unit = {
        // Initialize Spark Session with Hive support
        val spark = SparkSession.builder()
            .appName("sachetz_batch_contentbasedrecs")
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

        val averageVectors = udf(
            (vectors: Seq[Vector]) => {
                if (vectors == null || vectors.isEmpty) {
                    Vectors.dense(Array.empty[Double])
                } else {
                    // Initialize an array to hold the sum of each dimension
                    val sumArray = Array.fill(vectors.head.size)(0.0)

                    // Sum each dimension across all vectors
                    vectors.foreach { vector =>
                        vector.toArray.zipWithIndex.foreach { case (value, idx) =>
                            sumArray(idx) += value
                        }
                    }

                    // Compute the average for each dimension
                    val count = vectors.size.toDouble
                    val avgArray = sumArray.map(_ / count)

                    Vectors.dense(avgArray)
                }
            }
        )

        // Load User Actions and Filter for High Ratings
        val highRatingThreshold = 3.5
        val userActions = spark.sql("SELECT user_id, song_id, rating FROM sachetz_user_actions")
            .filter($"rating" >= highRatingThreshold)

        // Join with MSD Optimised to Get Song Features
        val msdFeatures = spark.sql("SELECT * FROM sachetz_msd")
            .select("song_id", "artist_hotttnesss", "danceability", "duration", "energy",
                "loudness", "tempo", "song_hotttnesss", "title", "artist_name", "album_name", "year")
        val userLikedSongs = userActions.join(msdFeatures, "song_id")

        // Assemble Features into Vectors
        val featureCols = Array("artist_hotttnesss", "danceability", "duration", "energy",
            "loudness", "tempo", "song_hotttnesss")
        val assembler = new VectorAssembler()
            .setInputCols(featureCols)
            .setOutputCol("features")
        val userFeatures = assembler.transform(userLikedSongs)
            .select("user_id", "song_id", "features")

        // Compute Average Feature Vector per User
        // Note: Spark ML's Vector types don't support averaging directly, so use UDF
        val avgUserFeatures = userFeatures.groupBy("user_id")
            .agg(
                averageVectors(collect_list("features")).alias("avg_features")
            )

        // Prepare All Songs with Feature Vectors
        val allSongs = msdFeatures.select("song_id", "artist_hotttnesss", "danceability", "duration",
            "energy", "loudness", "tempo", "song_hotttnesss",
            "title", "artist_name", "album_name", "year")
        val allSongsWithFeatures = assembler.transform(allSongs)
            .select("song_id", "title", "artist_name", "album_name", "year", "features")

        // UDF to Compute Cosine Similarity
        val cosineSimilarityUDF = udf((vec1: Vector, vec2: Vector) => {
            val dotProduct = vec1.toArray.zip(vec2.toArray).map { case (a, b) => a * b }.sum
            val normA = math.sqrt(vec1.toArray.map(a => a * a).sum)
            val normB = math.sqrt(vec2.toArray.map(b => b * b).sum)
            if (normA != 0 && normB != 0) dotProduct / (normA * normB) else 0.0
        })

        // Join User Average Features with All Songs to Compute Similarity
        val userSongSimilarity = avgUserFeatures.join(allSongsWithFeatures)
            .withColumn("similarity", cosineSimilarityUDF($"avg_features", $"features"))

        // Register the UDF
        spark.udf.register("cosineSimilarity", cosineSimilarityUDF)

        // Generate Top-N Recommendations per User Using Window Functions
        val windowSpec = Window.partitionBy("user_id").orderBy(desc("similarity"))

        val recommendations = userSongSimilarity
            .filter($"similarity" > 0.0) // Filter out non-similar songs
            .withColumn("rank", row_number().over(windowSpec))
            .filter($"rank" <= 10)
            .select(
                concat_ws("#", $"user_id", $"song_id").alias("row_key"), // Create row_key
                $"title".alias("song_name"),
                $"artist_name",
                $"album_name",
                $"year"
            )

        // Insert data into the Hive table
        recommendations.write
            .mode("overwrite") // Use "append" if you don't want to overwrite the table
            .insertInto("sachetz_ContentBasedRecs_hive")

        // Stop Spark Session
//        hive.close()
        spark.stop()
    }
}