import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.util.Bytes
import com.hortonworks.hwc.HiveWarehouseSession

object PopularityBasedRecs {
    def main(args: Array[String]): Unit = {
        // Initialize Spark Session with Hive support
        val spark = SparkSession.builder()
            .appName("sachetz_batch_popularitybasedrecs")
            .getOrCreate()
        val hive = HiveWarehouseSession.session(spark).build()
        hive.setDatabase("default")

        val sachetz_user_actions = hive.table("sachetz_user_actions")
        sachetz_user_actions.createOrReplaceTempView("sachetz_user_actions")
        val sachetz_msd_optimised = hive.table("sachetz_msd_optimised")
        sachetz_msd_optimised.createOrReplaceTempView("sachetz_msd_optimised")

        import spark.implicits._

        // Load MSD Optimised Table
        val msd = spark.sql("SELECT song_id, title, artist_name, album_name, year, song_hotttnesss, artist_hotttnesss FROM sachetz_msd_optimised")
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
            .select("song_id", "title", "artist_name", "album_name", "year", "popularity_score")

        // Save Recommendations to HBase
        // HBase Configuration
        val hbaseConf = HBaseConfiguration.create()
        hbaseConf.set("hbase.zookeeper.quorum","mpcs530132017test-hgm1-1-20170924181440.c.mpcs53013-2017.internal,mpcs530132017test-hgm2-2-20170924181505.c.mpcs53013-2017.internal,mpcs530132017test-hgm3-3-20170924181529.c.mpcs53013-2017.internal")
        hbaseConf.set("zookeeper.znode.parent", "/hbase-unsecure")
        hbaseConf.set("hbase.zookeeper.property.clientPort", "2181") // Default port

        // Function to write a partition of recommendations to HBase
        def writePartitionToHBase(partition: Iterator[org.apache.spark.sql.Row]): Unit = {
            val connection = ConnectionFactory.createConnection(hbaseConf)
            val table = connection.getTable(TableName.valueOf("PopularRecs"))

            partition.foreach { row =>
                val songId = row.getAs[String]("song_id")
                val songName = row.getAs[String]("title")
                val artistName = row.getAs[String]("artist_name")
                val albumName = row.getAs[String]("album_name")
                val year = row.getAs[Int]("year")

                // Row Key: song_id
                val rowKey = songId

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
        topPopularSongs.foreachPartition(writePartitionToHBase _)

        // Stop Spark Session
        spark.stop()
    }
}