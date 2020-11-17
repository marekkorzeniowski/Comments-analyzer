package Common

import Common.Post.replaceQuotes
import NLP.SentimentAnalysis

import scala.xml.Elem

case class Comment(override val rowKey: String,
                  commentId: Long,
                  postId: Long,
                  score: Int,
                  sentiment: String,
                  text: String,
                  creationDate: String,
                  userId: Long
                  ) extends PostObject (rowKey)

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
}

