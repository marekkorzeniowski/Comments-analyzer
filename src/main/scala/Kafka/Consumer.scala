package Kafka

import java.io.File
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
//    .config("spark.cassandra.connection.host", "localhost") //192.168.100.2    172.24.0.6
    .getOrCreate()



  val ssc = new StreamingContext(spark.sparkContext, Seconds(5))


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
    ConsumerStrategies.Subscribe[String, String](topics, kafkaParams + ("group.id" -> "group2"))
    )

    val processedStream = kafkaDStream.map {record =>

      val xml = XML.loadString(record.value())
      val postid = xml.attribute("PostId").getOrElse(0).toString.toInt
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
  import spark.implicits._

  def saveAsCsv() = {
    readFromKafka().foreachRDD {rdd =>
      val ds = spark.createDataset(rdd)  // encoder requried (Encoders.product[Class]) or import sprak.implicits._
      val f = new File("/home/marek/Repos/Comments-analyzer/src/main/resources/data/comments") // only to inspect how many files inside
      val nFiles = f.listFiles().length
      val path = s"/home/marek/Repos/Comments-analyzer/src/main/resources/data/comments/comment$nFiles.csv"

      ds.write.csv(path)
    }
  }


  def main(args: Array[String]): Unit = {

    readFromKafka().print(1000)

//    saveAsCsv()

    ssc.start()
    ssc.awaitTermination()

  }


}
