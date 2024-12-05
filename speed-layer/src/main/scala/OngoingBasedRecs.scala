import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.{DefaultScalaModule, ScalaObjectMapper}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.slf4j.LoggerFactory

object OngoingBasedRecs {
    // Initialize Logger
    val logger = LoggerFactory.getLogger(this.getClass)

    // Initialize Jackson ObjectMapper for JSON parsing
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

    // HBase configuration for writing recommendations
    val hbaseConf: Configuration = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hbaseConf.set("hbase.zookeeper.quorum", "localhost")

    // HBase table name for recommendations
    val RECS_TABLE = "sachetz_OngoingRecs"

    def main(args: Array[String]): Unit = {
        if (args.length < 2) {
            System.err.println(
                """
                  |Usage: OngoingBasedRecs <brokers> <batchIntervalSeconds>
                  |  <brokers> is a list of one or more Kafka brokers
                  |  <batchIntervalSeconds> is the batch interval for Spark Streaming
                  |
            """.stripMargin)
            System.exit(1)
        }

        val Array(brokers, batchIntervalStr) = args
        val batchInterval = Seconds(batchIntervalStr.toInt)

        // Initialize Spark Streaming Context with checkpointing
        val sparkConf = new SparkConf().setAppName("sachetz_speed_ongoingbasedrecs")
        val ssc = new StreamingContext(sparkConf, batchInterval)

        // Kafka parameters
        val topicsSet = Set("sachetz_user_actions_topic")
        val kafkaParams = Map[String, Object](
            "bootstrap.servers" -> brokers,
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> "lambda_speed_layer_group",
            "auto.offset.reset" -> "latest",
            "enable.auto.commit" -> (false: java.lang.Boolean)
        )

        // Create direct Kafka stream
        val kafkaStream = KafkaUtils.createDirectStream[String, String](
            ssc,
            LocationStrategies.PreferConsistent,
            ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams)
        )

        // Deserialize JSON messages to UserAction
        kafkaStream.foreachRDD { rdd =>
            if (!rdd.isEmpty()) {
                // Retrieve offset ranges
                val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

                try {
                    // Deserialize and filter user actions
                    val userActions = rdd.map(record => {
                        try {
                            val temp = mapper.readValue[Map[String, Any]](record.value())
                            UserAction(
                                userId = temp("userId").asInstanceOf[String],
                                songId = temp("songId").asInstanceOf[String],
                                rating = temp("rating").toString.toDouble,
                                action = temp("action").asInstanceOf[String],
                                timestamp = temp("timestamp").toString.toLong
                            )
                        } catch {
                            case e: Exception =>
                                logger.error(s"Failed to parse record: ${record.value()}", e)
                                null
                        }
                    }).filter(_ != null)

                    // Initialize SparkSession (reuse if possible)
                    val spark = org.apache.spark.sql.SparkSession.builder
                        .config(rdd.sparkContext.getConf)
                        .enableHiveSupport()
                        .getOrCreate()
                    import spark.implicits._

                    // Write user actions to Hive (Batch Layer)
                    val actionsDF = userActions.map(action => (
                        action.userId,
                        action.songId,
                        action.action,
                        new java.sql.Timestamp(action.timestamp),
                        action.rating
                    )).toDF("user_id", "song_id", "action_type", "action_time", "rating")

                    actionsDF.write.mode("append").insertInto("sachetz_user_actions")

                    // Generate Recommendations using ALS
                    val playedActions = userActions.filter(_.action == "played").map(action => {
                        // Convert userId and songId to unique integer IDs
                        Rating(action.userId.hashCode, action.songId.hashCode, action.rating)
                    })

                    val playedActionsCount = playedActions.count()
                    if (playedActionsCount >= 5) { // Adjust the threshold as needed
                        // Train ALS model
                        val rank = 10
                        val numIterations = 5
                        val lambda = 0.01
                        val alsModel = ALS.train(playedActions, rank, numIterations, lambda)

                        // Generate top 10 recommendations for all users
                        val userRecs = alsModel.recommendProductsForUsers(10)

                        // Flatten the recommendations
                        val flattenedRecs: RDD[(String, String, Double)] = userRecs.flatMap { case (_, recs) =>
                            recs.map(rec => (
                                rec.user.toString,
                                rec.product.toString,
                                rec.rating
                            ))
                        }

                        // Enrich recommendations with song metadata
                        val songMetadataDF = spark.table("sachetz_msd").select("song_id", "title", "artist_name", "album_name", "year")
                        val songMetadata = songMetadataDF.rdd.map(row => (
                            row.getAs[String]("song_id"),
                            (row.getAs[String]("title"), row.getAs[String]("artist_name"), row.getAs[String]("album_name"), row.getAs[Int]("year"))
                        )).collectAsMap()

                        val songMetadataBroadcast = ssc.sparkContext.broadcast(songMetadata)

                        val enrichedRecs: RDD[SongMetadata] = flattenedRecs.map { case (userId, songId, _) =>
                            val metadata = songMetadataBroadcast.value.getOrElse(songId, ("Unknown", "Unknown", "Unknown", 0))
                            SongMetadata(
                                user_id = userId,
                                song_id = songId,
                                song_name = metadata._1,
                                artist_name = metadata._2,
                                album_name = metadata._3,
                                year = metadata._4
                            )
                        }

                        // Write to HBase using direct Put operations
                        enrichedRecs.foreachPartition { partition =>
                            // Initialize HBase connection and table inside the partition
                            var connection: Connection = null
                            var table: Table = null
                            try {
                                connection = ConnectionFactory.createConnection(hbaseConf)
                                table = connection.getTable(TableName.valueOf(RECS_TABLE))
                                partition.foreach { rec =>
                                    // Create composite row key: userId_songId
                                    val rowKey = s"${rec.user_id}_${rec.song_id}"
                                    val put = new Put(Bytes.toBytes(rowKey))
                                    put.addColumn(Bytes.toBytes("details"), Bytes.toBytes("song_name#b"), Bytes.toBytes(rec.song_name))
                                    put.addColumn(Bytes.toBytes("details"), Bytes.toBytes("artist_name#b"), Bytes.toBytes(rec.artist_name))
                                    put.addColumn(Bytes.toBytes("details"), Bytes.toBytes("album_name#b"), Bytes.toBytes(rec.album_name))
                                    put.addColumn(Bytes.toBytes("details"), Bytes.toBytes("year#b"), Bytes.toBytes(rec.year.toString)) // Store year as String
                                    table.put(put)
                                }
                            } catch {
                                case e: Exception =>
                                    logger.error("Error writing to HBase", e)
                            } finally {
                                if (table != null) table.close()
                                if (connection != null && !connection.isClosed) connection.close()
                            }
                        }

                        logger.info(s"${playedActionsCount} recommendations added to HBase")
                    } else {
                        logger.info(s"${playedActionsCount} recommendations skipped due to low song count")
                    }

                    // Commit Offsets after successful processing
                    kafkaStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
                    logger.info(s"Successfully processed and committed offsets")
                } catch {
                    case e: Exception =>
                        logger.error("Error processing RDD, offsets not committed", e)
                    // Offsets are not committed, allowing for reprocessing
                }
            }
        }

        ssc.start()
        ssc.awaitTermination()
    }
}