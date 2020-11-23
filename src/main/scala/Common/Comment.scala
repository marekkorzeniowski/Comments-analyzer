package Common

import Common.Post.replaceQuotes
import NLP.SentimentAnalysis
import Spark_App.Consumer.spark
import org.apache.spark.sql.Encoders
import org.apache.spark.streaming.dstream.DStream

import scala.xml.Elem

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
  def processComments(rowKey: String, xmlRecord: Elem): Comment = {

    val commentId = xmlRecord.attribute("Id").getOrElse(-1).toString.toLong
    val postId = xmlRecord.attribute("PostId").getOrElse(0).toString.toLong
    val dateTime = xmlRecord.attribute("CreationDate").getOrElse("0001-01-01T00:00:00.000").toString
    val score = xmlRecord.attribute("Score").getOrElse(-1).toString.toInt
    val text = xmlRecord.attribute("Text").getOrElse("N/A").toString
    val parsedText = replaceQuotes(text)
    val sentiment = SentimentAnalysis.detectSentiment(parsedText).toString
    val userId = xmlRecord.attribute("UserId").getOrElse(-1).toString.toLong

    Comment(rowKey, commentId, postId, score, sentiment, parsedText, dateTime, userId)
  }

  def saveCommentAsCsv(dStream: DStream[Comment], path: String): Unit = {
    var rddNumber = 1
    dStream.foreachRDD { rdd =>

      val ds = spark.createDataset(rdd)(Encoders.product[Comment])

      ds.write.csv(s"$path/$rddNumber")
      rddNumber += 1
    }
  }
}

