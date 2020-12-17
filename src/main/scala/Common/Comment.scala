package Common

import Main.Consumer.{spark, ssc}
import NLP.SentimentAnalysis.{getScore, tokenizer}
import NLP.SentimentType
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Dataset, Encoders}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import java.sql.Timestamp
import scala.util.Try
import scala.xml.{Elem, XML}

case class Comment(rowKey: String,
                   commentId: Long,
                   postId: Long,
                   score: Int,
                   sentiment_label: String,
                   avg_score: Float,
                   pos_score: Int,
                   neg_score:Int,
                   tot_score: Int,
                   text: String,
                   creationDate: Timestamp,
                   userId: Long
                  )

case class CommentWithLocation(rowKey: String,
                               commentId: Long,
                               postId: Long,
                               score: Int,
                               sentiment_label: String,
                               avg_score: Float,
                               pos_score: Int,
                               neg_score:Int,
                               tot_score: Int,
                               text: String,
                               creationDate: Timestamp,
                               userId: Long,
                               userName: String,
                               location: String)



object Comment {

  def processComments(xmlRecord: Elem, pos_words: Broadcast[Map[String, Int]],
                      neg_words: Broadcast[Map[String, Int]]): Comment = {

    val commentId = xmlRecord.attribute("Id").getOrElse(-1).toString.toLong
    val postId = xmlRecord.attribute("PostId").getOrElse(0).toString.toLong
    val dateTime = xmlRecord.attribute("CreationDate").getOrElse("0001-01-01 00:00:00.0").toString
    val timeStamp = Common.getTimeStampFromString(dateTime)
    val score = xmlRecord.attribute("Score").getOrElse(-1).toString.toInt
    val text = xmlRecord.attribute("Text").getOrElse("N/A").toString
    val parsedText = Common.parseTitle(text)
    val pos_tokens = tokenizer(parsedText, pos_words)
    val pos_score = getScore(pos_tokens)
    val neg_tokens = tokenizer(parsedText, neg_words)
    val neg_score = getScore(neg_tokens)
    val tot_score = pos_score - neg_score
    val avg_score = Try(tot_score.toFloat/pos_tokens.length).getOrElse(0f)
    val sentiment_label = SentimentType.fromScore(avg_score).toString
    val userId = xmlRecord.attribute("UserId").getOrElse(-1).toString.toLong

    Comment("Comment", commentId, postId, score,
      sentiment_label, avg_score, pos_score, neg_score, tot_score,
      parsedText, timeStamp, userId)
  }

  def readFromKafkaComments(topic: String, kafkaParams: Map[String, Object],
                            pos_words: Broadcast[Map[String, Int]],
                            neg_words: Broadcast[Map[String, Int]]): DStream[Comment] = {

    val topics = Array(topic)
    val kafkaDStream = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent, //Distributes the partitions evenly across the Spark cluster
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams + ("group.id" -> "group1"))
    )
    val processedStream = kafkaDStream.map { record =>
      val xml = XML.loadString(record.value())
      processComments(xml, pos_words, neg_words)
    }
    processedStream
  }

  def joinCommentsWithStaticData(dStream: DStream[Comment], users: Dataset[(Long, User)]): DStream[CommentWithLocation] = {
    val usersRDD = users.rdd
    val dStreamTuple = dStream.map(comment => (comment.userId, comment))

    val joinedDStream = dStreamTuple.transform(stream => stream.leftOuterJoin(usersRDD))

    joinedDStream.map{case (id, (comment, user)) =>
      CommentWithLocation(
        comment.rowKey,
        comment.commentId,
        comment.postId,
        comment.score,
        comment.sentiment_label,
        comment.avg_score,
        comment.pos_score,
        comment.neg_score,
        comment.tot_score,
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
      val timeStamp = Common.getCurrentTimeStamp

      ds.write.option("header", value = true).option("delimiter", "@#$")
        .csv(s"$path/$timeStamp")
    }
  }

  def saveCommentAsParquet(dStream: DStream[CommentWithLocation], path: String): Unit = {
    dStream.foreachRDD { rdd =>
      val ds = spark.createDataset(rdd)(Encoders.product[CommentWithLocation])

      val timeStamp = Common.getCurrentTimeStamp
      ds.write.parquet(s"$path/$timeStamp")
    }
  }
}

