package Common

import NLP.SentimentAnalysis

import scala.xml.Elem

abstract class PostObject (val rowKey: String) extends Product with Serializable

case class Post (override val rowKey: String,
                 postId: Long,
                 postTypeId: Int,
                 parentId: Int,
                 creationDateTime: String,
                 score: Int,
                 viewCount: Long,
                 title: String,
                 sentiment: String,
                 body: String,
                 ownerUserId: Long,
                 tags: String,
                 answerCount: Int,
                 commentCount: Int,
                 favoriteCount: Int
               ) extends PostObject (rowKey)

object Post {
  def processPosts(rowKey: String ,xmlRecord: Elem): Post = {

    val postId = xmlRecord.attribute("Id").getOrElse(-1).toString.toLong
    val postTypeId = xmlRecord.attribute("PostTypeId").getOrElse(-1).toString.toInt
    val parentId = xmlRecord.attribute("ParentID").getOrElse(-1).toString.toInt
    val creationDateTime = xmlRecord.attribute("CreationDate").getOrElse("0001-01-01T00:00:00.000").toString
    val score = xmlRecord.attribute("Score").getOrElse(-1).toString.toInt
    val viewCount = xmlRecord.attribute("ViewCount").getOrElse(-1).toString.toLong
    val title = xmlRecord.attribute("Title").getOrElse("N/A").toString
    val parsedTitle = replaceQuotes(title)
    val body = xmlRecord.attribute("Body").getOrElse("N/A").toString
    val parsedBody = parseBody(body)
    val sentiment = SentimentAnalysis.detectSentiment(parsedBody).toString
    val ownerUserId = xmlRecord.attribute("OwnerUserId").getOrElse(-1).toString.toLong
    val tags = xmlRecord.attribute("Tags").getOrElse("N/A").toString
    val parsedTags = parseTags(tags)
    val answerCount = xmlRecord.attribute("AnswerCount").getOrElse(-1).toString.toInt
    val commentCount = xmlRecord.attribute("CommentCount").getOrElse(-1).toString.toInt
    val favoriteCount = xmlRecord.attribute("FavoriteCount").getOrElse(-1).toString.toInt

    Post(rowKey, postId, postTypeId, parentId, creationDateTime, score,
      viewCount, parsedTitle, sentiment, parsedBody, ownerUserId,
      parsedTags, answerCount, commentCount, favoriteCount)
  }

  def parseBody(body: String): String = {
    val regex = "&lt;(.+?)&gt;".r

    val processed = regex.replaceAllIn(body, "").replaceAll("\\s+", " ")
    replaceQuotes(processed)
  }

  def parseTags(tags: String): String =  tags
    .replace("&lt;", "")
    .replace("&gt;", " ")
    .replaceAll("\\s+", " ")

  def replaceQuotes(text: String) = text.replaceAll("&quot;", "\"")
}
