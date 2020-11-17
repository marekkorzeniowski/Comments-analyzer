package Spark_App

import Common.Comment.processComments
import Common.Post.processPosts
import Common.{Comment, Post, PostObject}
import Kafka.Producer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.xml.XML

object Consumer {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("comment-analyzer")
    .master("local[2]") //2 TODO local[2] = appropriate for aws?
    .getOrCreate()


  val ssc = new StreamingContext(spark.sparkContext, Seconds(5))


  def readFromKafka(topic1: String, topic2: String, kafkaParams: Map[String, Object]): DStream[PostObject] = {

    Producer.dataProducer() //3 TODO! Remove this in remote environment

    val topics = Array(topic1, topic2)
    val kafkaDStream = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent, //Distributes the partitions evenly across the Spark cluster
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams + ("group.id" -> "group1"))
    )

    val processedStream = kafkaDStream.map { record =>

      val rowKey = record.key()
      val xml = XML.loadString(record.value())
      rowKey match {
        case "Comment" => processComments(rowKey, xml)
        case "Post" => processPosts(rowKey, xml)
      }
    }
    processedStream
  }


//  import spark.implicits._ //4 TODO! Replace with S3 URL
  def saveAsCsv(topic1: String, topic2: String, kafkaParams: Map[String, Object], path: String): Unit = {
    var rddNumber = 1
    readFromKafka(topic1, topic2, kafkaParams).foreachRDD { rdd =>

      if (rdd.map(_.rowKey).toString() == "Post") {
        val castRDD = rdd.asInstanceOf[RDD[Post]]

        val ds = spark.createDataset(castRDD)(Encoders.product[Post])

        ds.write.csv(s"$path/$rddNumber")
        rddNumber += 1
      }
      else {
        val castRDD = rdd.asInstanceOf[RDD[Comment]]
        val ds = spark.createDataset(castRDD)(Encoders.product[Comment])

        ds.write.csv(s"$path/$rddNumber")
        rddNumber += 1
      }
    }
  }


  def main(args: Array[String]): Unit = {

    //    //1 TODO - replace parameters below with args
    //    if (args.length != 2) {
    //      println("Need 1) Kafka host   2) S3 output")
    //      System.exit(1)
    //    }

    val KAFKA_HOST = "localhost" //args(0) //
    val KAFKA_PORT = "9092"
    val KAFKA_TOPIC1 = "comment-analyzer"
    val KAFKA_TOPIC2 = "post-analyzer"
        val OUTPUT = "/home/marek/Repos/Comments-analyzer/src/main/resources/comment" //args(1)


    val KAFKA_PARAMS: Map[String, Object] = Map(
      "bootstrap.servers" -> s"$KAFKA_HOST:$KAFKA_PORT",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> false.asInstanceOf[Object]
    )

    println("Processing about to start")
    readFromKafka(KAFKA_TOPIC1, KAFKA_TOPIC2, KAFKA_PARAMS).print(1000)
//        saveAsCsv(KAFKA_TOPIC1,KAFKA_TOPIC2, KAFKA_PARAMS, OUTPUT)

    ssc.start()
    ssc.awaitTermination()

  }
}

