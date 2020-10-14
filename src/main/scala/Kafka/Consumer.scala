package Kafka

import java.time.LocalDateTime

import Common.Comment
import NLP.SentimentAnalysis
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import scala.xml.XML


object Consumer {

  val spark = SparkSession
    .builder()
    .appName("comment-analyzer")
    .master("local[2]")
    .config("spark.cassandra.connection.host", "localhost") //192.168.100.2    172.24.0.6
    .getOrCreate()


  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))


  val kafkaTopic = "comment-analyzer"

  val kafkaParams: Map[String, Object] = Map(
    "bootstrap.servers" -> "localhost:9092",
    "key.deserializer" -> classOf[StringDeserializer], // receiving data from kafka
    "value.deserializer" -> classOf[StringDeserializer],
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> false.asInstanceOf[Object]
  )


  def readFromKafka() = {
    Producer.dataProducer()

    val topics = Array(kafkaTopic)
    val kafkaDStream = KafkaUtils.createDirectStream(
    ssc,
    LocationStrategies.PreferConsistent, //Distributes the partitions evenly across the Spark cluster
    ConsumerStrategies.Subscribe[String, String](topics, kafkaParams + ("group.id" -> "group1"))
    )

    val processedStream = kafkaDStream.map {record =>

      val xml = XML.loadString(record.value())
      val postid = xml.attribute("postId").getOrElse(0).toString.toInt
      val date = xml.attribute("CreationDate").getOrElse("0001-01-01").toString
      val parsedDate = LocalDateTime.parse(date).toLocalDate
      val parsedTime = LocalDateTime.parse(date).toLocalTime
      val score = xml.attribute("Score").getOrElse(-1).toString.toInt

      val text = xml.attribute("Text").getOrElse("N/A").toString
      val nlp = SentimentAnalysis.detectSentiment(text)

      Comment(postid, score, nlp.toString, text)
    }
    processedStream

  }


  def main(args: Array[String]): Unit = {
    readFromKafka().print()
    ssc.start()
    ssc.awaitTermination()
  }




}
