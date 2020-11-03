package Spark_App

import Common.Comment
import NLP.SentimentAnalysis
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.xml.XML

object Consumer {
  //1 TODO - replace parameters below with args
  //  if (args.length != 3) {
  //    println("Need 1) big data source, 2) taxi zones data source, 3) output data destination")
  //    System.exit(1)
  //  }

  val KAFKA_HOST = "localhost"
  val KAFKA_PORT = "9092"
  val KAFKA_TOPIC = "comment-analyzer"

  val spark = SparkSession
    .builder()
    .appName("comment-analyzer")
    .master("local[2]") //2 TODO local[2] = appropriate for aws?
    .getOrCreate()


  val ssc = new StreamingContext(spark.sparkContext, Seconds(5))

//  classOf[StringDeserializer]       // "org.apache.kafka.common.serialization.StringDeserializer"
  val kafkaParams: Map[String, Object] = Map(
    "bootstrap.servers" -> s"$KAFKA_HOST:$KAFKA_PORT",
//    "key.serializer" -> classOf[StringSerializer], // send data to kafka
//    "value.serializer" -> classOf[StringSerializer],
    "key.deserializer" -> classOf[StringDeserializer], // receiving data from kafka
    "value.deserializer" -> classOf[StringDeserializer] ,
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> false.asInstanceOf[Object]
  )


  def readFromKafka() = {

//    Producer.dataProducer() //3 TODO! Remove this in remote environment

    val topics = Array(KAFKA_TOPIC)
    val kafkaDStream = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent, //Distributes the partitions evenly across the Spark cluster
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams + ("group.id" -> "group1"))
    )

    val processedStream = kafkaDStream.map { record =>

      val xml = XML.loadString(record.value())
      val postid = xml.attribute("PostId").getOrElse(0).toString.toInt
      val date = xml.attribute("CreationDate").getOrElse("0001-01-01").toString
      //      val parsedDate = LocalDateTime.parse(date).toLocalDate
      //      val parsedTime = LocalDateTime.parse(date).toLocalTime
      val score = xml.attribute("Score").getOrElse(-1).toString.toInt

      val text = xml.attribute("Text").getOrElse("N/A").toString
      val nlp = SentimentAnalysis.detectSentiment(text)

      Comment(postid, score, nlp.toString, text)
    }
    processedStream
  }


  import spark.implicits._

  def saveAsCsv(path: String): Unit = { //4 TODO! Replace with S3 URL
    readFromKafka().foreachRDD { rdd =>
      val ds = spark.createDataset(rdd) // encoder required (Encoders.product[Class]) or import spark.implicits._
//      val f = new File("/home/marek/Repos/Comments-analyzer/src/main/resources/data/comments") // only to inspect how many files inside
//      val nFiles = f.listFiles().length
//
      ds.write.csv(path)
    }
  }

//  def saveToS3() = { //5 TODO! How save to S3???
//    ???
//  }


  def main(args: Array[String]): Unit = {

//    val path = s"/home/marek/Repos/Comments-analyzer/src/main/resources/data/comments/comment$nFiles.csv"

//    val path = args(0)
    println("Processing about to start")
    readFromKafka().print(1000)

//    saveAsCsv(path)

    ssc.start()
    ssc.awaitTermination()

  }


}
