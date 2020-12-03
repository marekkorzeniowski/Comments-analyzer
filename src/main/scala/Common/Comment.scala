package Common

import NLP.{SentimentAnalysis, SentimentType}
import Main.Consumer.{spark, ssc}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Dataset, Encoders}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import scala.xml.{Elem, XML}

case class Comment(rowKey: String,
                  commentId: Long,
                  postId: Long,
                  score: Int,
                  sentiment: String,
                  text: String,
                  creationDate: String,
                  userId: Long
                  )

case class CommentWithLocation(rowKey: String,
                               commentId: Long,
                               postId: Long,
                               score: Int,
                               sentiment: String,
                               text: String,
                               creationDate: String,
                               userId: Long,
                               userName: String,
                               location: String)



object Comment {

  def processComments(rowKey: String, xmlRecord: Elem, lexicon: Broadcast[Map[String, Int]]): Comment = {

    val commentId = xmlRecord.attribute("Id").getOrElse(-1).toString.toLong
    val postId = xmlRecord.attribute("PostId").getOrElse(0).toString.toLong
    val dateTime = xmlRecord.attribute("CreationDate").getOrElse("0001-01-01T00:00:00.000").toString
    val score = xmlRecord.attribute("Score").getOrElse(-1).toString.toInt
    val text = xmlRecord.attribute("Text").getOrElse("N/A").toString
    val parsedText = Common.parseTitle(text)
    val tokenizer = SentimentAnalysis.tokenizer(parsedText, lexicon)
    val avgScore = SentimentAnalysis.calculateAvgScore(tokenizer)
    val sentiment = SentimentType.fromScore(avgScore).toString
    val userId = xmlRecord.attribute("UserId").getOrElse(-1).toString.toLong

    Comment(rowKey, commentId, postId, score, sentiment, parsedText, dateTime, userId)
  }


  def readFromKafkaComments(topic: String, kafkaParams: Map[String, Object], lexicon: Broadcast[Map[String, Int]]): DStream[Comment] = {

    val topics = Array(topic)
    val kafkaDStream = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent, //Distributes the partitions evenly across the Spark cluster
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams + ("group.id" -> "group1"))
    )

    val processedStream = kafkaDStream.map { record =>

      val xml = XML.loadString(record.value())
      processComments(record.key(), xml, lexicon)
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

  def saveCommentAsCsv(dStream: DStream[CommentWithLocation], path: String): Unit = {

    dStream.foreachRDD { rdd =>

      val ds = spark.createDataset(rdd)(Encoders.product[CommentWithLocation])

      val timeStamp = System.currentTimeMillis()
      ds.write.csv(s"$path/$timeStamp")
    }
  }
}

