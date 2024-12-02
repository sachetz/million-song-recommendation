import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.recommendation.ALS
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.util.Bytes
import com.hortonworks.hwc.HiveWarehouseSession

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

        // Save Recommendations to HBase
        // HBase Configuration
        val hbaseConf = HBaseConfiguration.create()
        hbaseConf.set("hbase.zookeeper.quorum","mpcs530132017test-hgm1-1-20170924181440.c.mpcs53013-2017.internal,mpcs530132017test-hgm2-2-20170924181505.c.mpcs53013-2017.internal,mpcs530132017test-hgm3-3-20170924181529.c.mpcs53013-2017.internal")
        hbaseConf.set("zookeeper.znode.parent", "/hbase-unsecure")
        hbaseConf.set("hbase.zookeeper.property.clientPort", "2181") // Default port

        // Function to write a partition of recommendations to HBase
        def writePartitionToHBase(partition: Iterator[org.apache.spark.sql.Row]): Unit = {
            val connection = ConnectionFactory.createConnection(hbaseConf)
            val table = connection.getTable(TableName.valueOf("ALSRecs"))

            partition.foreach { row =>
                val userId = row.getAs[String]("user_id")
                val songId = row.getAs[String]("song_id")
                val songName = row.getAs[String]("title")
                val artistName = row.getAs[String]("artist_name")
                val albumName = row.getAs[String]("album_name")
                val year = row.getAs[Int]("year")

                // Composite Row Key: user_id#song_id
                val rowKey = s"$userId#$songId"

                val put = new Put(Bytes.toBytes(rowKey))
                put.addColumn(Bytes.toBytes("details"), Bytes.toBytes("song_name"), Bytes.toBytes(songName))
                put.addColumn(Bytes.toBytes("details"), Bytes.toBytes("artist_name"), Bytes.toBytes(artistName))
                put.addColumn(Bytes.toBytes("details"), Bytes.toBytes("album_name"), Bytes.toBytes(albumName))
                put.addColumn(Bytes.toBytes("details"), Bytes.toBytes("year"), Bytes.toBytes(year.toString))

                table.put(put)
            }

            table.close()
            connection.close()
        }

        // Write to HBase using foreachPartition for efficiency
        recsWithDetails.foreachPartition(writePartitionToHBase _)

        // Stop Spark Session
        spark.stop()
    }
}