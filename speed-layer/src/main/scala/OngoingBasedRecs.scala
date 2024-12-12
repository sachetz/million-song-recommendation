import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.{DefaultScalaModule, ScalaObjectMapper}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.slf4j.LoggerFactory

import scala.collection.convert.ImplicitConversions.`collection asJava`
import scala.math.sqrt

// Case class for user actions
case class UserAction(
                         userId: String,
                         songId: String,
                         rating: Double,
                         action: String,
                         timestamp: Long
                     )

// Case class for song metadata
case class SongMetadata(
                           song_id: String,
                           title: String,
                           artist_name: String,
                           album_name: String,
                           year: Int,
                           danceability: Double,
                           duration: Double,
                           energy: Double,
                           loudness: Double,
                           mode: Int,
                           tempo: Double
                       )

// Case class for user-specific recommendations
case class UserRecommendation(
                                 user_id: String,
                                 song_id: String,
                                 title: String,
                                 artist_name: String,
                                 album_name: String,
                                 year: Int
                             )

object OngoingBasedRecs {
    // Initialize Logger
    val logger = LoggerFactory.getLogger(this.getClass)

    // Initialize Jackson ObjectMapper for JSON parsing
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

    // HBase configuration for writing recommendations
    val hbaseConf: Configuration = HBaseConfiguration.create()
    val hbaseConnection = ConnectionFactory.createConnection(hbaseConf)
    val hbaseTable = hbaseConnection.getTable(TableName.valueOf("sachetz_OngoingRecs"))

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

        // Initialize SparkSession
        val spark = SparkSession.builder.config(sparkConf).enableHiveSupport().getOrCreate()
        import spark.implicits._

        // Load and process song metadata once and broadcast it
        val songMetadataDF = spark.table("sachetz_msd").select(
            "song_id", "title", "artist_name", "album_name", "year",
            "danceability", "duration", "energy", "loudness", "mode", "tempo"
        )

        // Convert DataFrame to RDD of SongMetadata
        val songMetadataRDD = songMetadataDF.rdd.map(row => SongMetadata(
            song_id = row.getAs[String]("song_id"),
            title = row.getAs[String]("title"),
            artist_name = row.getAs[String]("artist_name"),
            album_name = row.getAs[String]("album_name"),
            year = row.getAs[Int]("year"),
            danceability = row.getAs[Double]("danceability"),
            duration = row.getAs[Double]("duration"),
            energy = row.getAs[Double]("energy"),
            loudness = row.getAs[Double]("loudness"),
            mode = row.getAs[Int]("mode"),
            tempo = row.getAs[Double]("tempo")
        )).cache()

        // Create a map of song_id to feature vector
        val songFeatures: Map[String, Array[Double]] = songMetadataRDD.map { song =>
            val features = Array(
                song.danceability,
                song.duration,
                song.energy,
                song.loudness,
                song.mode.toDouble,
                song.tempo
            )
            (song.song_id, features)
        }.collectAsMap().toMap

        // Broadcast the song features map for efficient lookup
        val songFeaturesBroadcast: Broadcast[Map[String, Array[Double]]] = ssc.sparkContext.broadcast(songFeatures)

        // Broadcast song metadata for enrichment
        val songMetadataMap: Map[String, SongMetadata] = songMetadataRDD.map(song => (song.song_id, song)).collectAsMap().toMap
        val songMetadataBroadcast: Broadcast[Map[String, SongMetadata]] = ssc.sparkContext.broadcast(songMetadataMap)

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

        // Function to compute cosine similarity between two vectors
        def cosineSimilarity(vec1: Array[Double], vec2: Array[Double]): Double = {
            val dotProduct = vec1.zip(vec2).map { case (a, b) => a * b }.sum
            val normA = sqrt(vec1.map(x => x * x).sum)
            val normB = sqrt(vec2.map(x => x * x).sum)
            if (normA == 0.0 || normB == 0.0) 0.0 else dotProduct / (normA * normB)
        }

        // Function to compute average feature vector for a user's interacted songs
        def averageUserVector(songIds: Iterable[String], songFeaturesMap: Map[String, Array[Double]]): Option[Array[Double]] = {
            val vectors = songIds.flatMap(songFeaturesMap.get)
            if (vectors.isEmpty) None
            else {
                val summed = vectors.reduce((a, b) => a.zip(b).map { case (x, y) => x + y })
                Some(summed.map(_ / vectors.size))
            }
        }

        // Deserialize JSON messages to UserAction and process
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

                    // Write user actions to Hive (Batch Layer)
                    val actionsDF = userActions.map(action => (
                        action.userId,
                        action.songId,
                        action.action,
                        new java.sql.Timestamp(action.timestamp),
                        action.rating
                    )).toDF("user_id", "song_id", "action_type", "action_time", "rating")

                    actionsDF.write.mode("append").insertInto("sachetz_user_actions")

                    // Generate Content-Based Recommendations Per User
                    val playedActionsByUser: RDD[(String, Iterable[String])] = userActions
                        .filter(_.action == "listened")
                        .map(action => (action.userId, action.songId))
                        .groupByKey()

                    val songFeaturesMap = songFeaturesBroadcast.value
                    val userMetadataMap = songMetadataBroadcast.value

                    playedActionsByUser.foreachPartition { partition =>
                        try {
                            partition.foreach { case (userId, playedSongIds) =>
                                // Compute average user vector
                                val userVectorOpt = averageUserVector(playedSongIds, songFeaturesMap)

                                userVectorOpt.foreach { userVector =>
                                    // Compute similarity for all songs
                                    val similarities = songFeaturesMap.map { case (songId, features) =>
                                        val sim = cosineSimilarity(userVector, features)
                                        (songId, sim)
                                    }

                                    // Get top 10 similar songs excluding already played
                                    val topRecs = similarities
                                        .filter { case (songId, _) => !playedSongIds.contains(songId) }
                                        .toSeq
                                        .sortBy(-_._2)
                                        .take(10)

                                    // Enrich recommendations with song metadata
                                    val enrichedRecs: Seq[UserRecommendation] = topRecs.flatMap { case (songId, _) =>
                                        userMetadataMap.get(songId).map { song =>
                                            UserRecommendation(
                                                user_id = userId,
                                                song_id = song.song_id,
                                                title = song.title,
                                                artist_name = song.artist_name,
                                                album_name = song.album_name,
                                                year = song.year
                                            )
                                        }
                                    }

                                    // Write to HBase
                                    enrichedRecs.foreach { rec =>
                                        // Create row key: userId_songId
                                        val rowKey = s"${rec.user_id}#${rec.song_id}"
                                        val put = new Put(Bytes.toBytes(rowKey))
                                        put.addColumn(Bytes.toBytes("details"), Bytes.toBytes("song_name#b"), Bytes.toBytes(rec.title))
                                        put.addColumn(Bytes.toBytes("details"), Bytes.toBytes("artist_name#b"), Bytes.toBytes(rec.artist_name))
                                        put.addColumn(Bytes.toBytes("details"), Bytes.toBytes("album_name#b"), Bytes.toBytes(rec.album_name))
                                        put.addColumn(Bytes.toBytes("details"), Bytes.toBytes("year#b"), Bytes.toBytes(rec.year.toString)) // Store year as String
                                        hbaseTable.put(put)
                                    }

                                    logger.info(s"Added ${enrichedRecs.size} recommendations to HBase for user: $userId")
                                }
                            }
                        } catch {
                            case e: Exception =>
                                logger.error("Error writing to HBase", e)
                        }
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