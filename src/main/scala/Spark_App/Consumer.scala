package Spark_App

import Common.Comment.{processComments, saveCommentAsCsv}
import Common.Post.{processPosts, savePostAsCsv}
import Common._
import Kafka.Producer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.{Dataset, SparkSession}
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


  def readFromKafkaComments(topic: String, kafkaParams: Map[String, Object]): DStream[Comment] = {

  //3 TODO! Remove this in remote environment
    val topics = Array(topic)
    val kafkaDStream = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent, //Distributes the partitions evenly across the Spark cluster
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams + ("group.id" -> "group1"))
    )

    val processedStream = kafkaDStream.map { record =>

      val xml = XML.loadString(record.value())
      processComments(record.key(), xml)
    }
    processedStream
  }

  def joinCommentsWithStaticData(dStream: DStream[Comment], users: Dataset[(Long, User)]): DStream[CommentWithLocation] = {

    val usersRDD = users.rdd
    val dStreamTuple = dStream.map(comment => (comment.userId, comment))

    val joinedDStream = dStreamTuple.transform(stream => stream.leftOuterJoin(usersRDD))

    joinedDStream.map{case (id, (comment, user)) =>
      CommentWithLocation(comment.rowKey,
                          comment.commentId,
                          comment.postId,
                          comment.score,
                          comment.sentiment,
                          comment.text,
                          comment.creationDate,
                          comment.userId,
                          user.get.name.getOrElse("Missing user name"),
                          user.get.location.getOrElse("Missing location"))
    }
  }

  def readFromKafkaPosts(topic: String, kafkaParams: Map[String, Object]): DStream[Post] = {
 //3 TODO! Remove this in remote environment
    val topics = Array(topic)
    val kafkaDStream = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent, //Distributes the partitions evenly across the Spark cluster
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams + ("group.id" -> "group2"))
    )

    val processedStream = kafkaDStream.map { record =>

      val xml = XML.loadString(record.value())
      processPosts(record.key(), xml)
    }
    processedStream
  }

  def joinPostsWithStaticData(dStream: DStream[Post], users: Dataset[(Long, User)]): DStream[PostWithLocation] = {

    val usersRDD = users.rdd
    val dStreamTuple = dStream.map(post => (post.ownerUserId, post))

    val joinedDStream = dStreamTuple.transform(stream => stream.leftOuterJoin(usersRDD))

    joinedDStream.map{case (id, (post, user)) =>
      PostWithLocation(post.rowKey,
                       post.postId,
                       post.postTypeId,
                       post.parentId,
                       post.creationDateTime,
                       post.score,
                       post.viewCount,
                       post.title,
                       post.sentiment,
                       post.body,
                       post.ownerUserId,
                       post.tags,
                       post.answerCount,
                       post.commentCount,
                       post.favoriteCount,
                       user.get.name.getOrElse("Missing user name"),
                       user.get.location.getOrElse("Missing location"))
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
    val KAFKA_TOPIC1 = "comment-analyzer"  //args(1)
    val KAFKA_TOPIC2 = "post-analyzer"    //args(2)
    val USERS_DF = "/home/marek/Repos/Comments-analyzer/src/main/resources/data/users_parquet"  //args(3)
    val OUTPUT1 = "/home/marek/Repos/Comments-analyzer/src/main/resources/comments" //args(4)
    val OUTPUT2 = "/home/marek/Repos/Comments-analyzer/src/main/resources/posts" //args(5)



    val KAFKA_PARAMS: Map[String, Object] = Map(
      "bootstrap.servers" -> s"$KAFKA_HOST:$KAFKA_PORT",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> false.asInstanceOf[Object]
    )

    println("Processing about to start")

    import spark.implicits._
    val users = spark.read.parquet(USERS_DF).as[(Long, User)]
    Producer.dataProducer()                   // TODO - remove in remote version


    val dStream1 = readFromKafkaComments(KAFKA_TOPIC1, KAFKA_PARAMS)
    val joinedDStream1 = joinCommentsWithStaticData(dStream1, users)
    joinedDStream1.print(1000)

    saveCommentAsCsv(dStream1, OUTPUT1)


    val dStream2 = readFromKafkaPosts(KAFKA_TOPIC2, KAFKA_PARAMS)
    val joinedDStream2 = joinPostsWithStaticData(dStream2, users)
    joinedDStream2.print(1000)

    savePostAsCsv(joinedDStream2, OUTPUT2)

    ssc.start()
    ssc.awaitTermination()

  }
}

