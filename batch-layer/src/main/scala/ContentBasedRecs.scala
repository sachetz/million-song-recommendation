import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vector
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.util.Bytes
import com.hortonworks.hwc.HiveWarehouseSession

object ContentBasedRecs {
    def main(args: Array[String]): Unit = {
        // Initialize Spark Session with Hive support
        val spark = SparkSession.builder()
            .appName("sachetz_batch_contentbasedrecs")
            .getOrCreate()
        val hive = HiveWarehouseSession.session(spark).build()
        hive.setDatabase("default")

        val sachetz_user_actions = hive.table("sachetz_user_actions")
        sachetz_user_actions.createOrReplaceTempView("sachetz_user_actions")
        val sachetz_msd_optimised = hive.table("sachetz_msd_optimised")
        sachetz_msd_optimised.createOrReplaceTempView("sachetz_msd_optimised")

        import spark.implicits._

        // Load User Actions and Filter for High Ratings
        val highRatingThreshold = 3.5
        val userActions = spark.sql("SELECT user_id, song_id, rating FROM sachetz_user_actions")
            .filter($"rating" >= highRatingThreshold)

        // Join with MSD Optimised to Get Song Features
        val msdFeatures = spark.sql("SELECT * FROM sachetz_msd_optimised")
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
                avg("features").alias("avg_features")
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
                $"user_id",
                $"song_id",
                $"title",
                $"artist_name",
                $"album_name",
                $"year",
                $"similarity"
            )

        // Save Recommendations to HBase
        // HBase Configuration
        val hbaseConf = HBaseConfiguration.create()
        hbaseConf.set("hbase.zookeeper.quorum","mpcs530132017test-hgm1-1-20170924181440.c.mpcs53013-2017.internal,mpcs530132017test-hgm2-2-20170924181505.c.mpcs53013-2017.internal,mpcs530132017test-hgm3-3-20170924181529.c.mpcs53013-2017.internal")
        hbaseConf.set("zookeeper.znode.parent", "/hbase-unsecure")
        hbaseConf.set("hbase.zookeeper.property.clientPort", "2181") // Default port

        val connection = ConnectionFactory.createConnection(hbaseConf)
        val table = connection.getTable(TableName.valueOf("ContentBasedRecs"))

        // Function to write a single recommendation to HBase
        def writeToHBase(row: org.apache.spark.sql.Row): Unit = {
            val userId = row.getAs[String]("user_id")
            val songId = row.getAs[String]("song_id")
            val songName = row.getAs[String]("title")
            val artistName = row.getAs[String]("artist_name")
            val albumName = row.getAs[String]("album_name")
            val year = row.getAs[Int]("year")

            val put = new Put(Bytes.toBytes(s"$userId#$songId"))
            put.addColumn(Bytes.toBytes("details"), Bytes.toBytes("song_name"), Bytes.toBytes(songName))
            put.addColumn(Bytes.toBytes("details"), Bytes.toBytes("artist_name"), Bytes.toBytes(artistName))
            put.addColumn(Bytes.toBytes("details"), Bytes.toBytes("album_name"), Bytes.toBytes(albumName))
            put.addColumn(Bytes.toBytes("details"), Bytes.toBytes("year"), Bytes.toBytes(year.toString))

            table.put(put)
        }

        // Collect and write to HBase
        recommendations.collect().foreach(writeToHBase)

        // Close HBase connections
        table.close()
        connection.close()

        // Stop Spark Session
        spark.stop()
    }
}