package Common

import NLP.SentimentAnalysis
import Spark_App.Consumer.spark
import org.apache.spark.sql.Encoders
import org.apache.spark.streaming.dstream.DStream

import scala.xml.Elem

case class Post (rowKey: String,
                 postId: Long,
                 postTypeId: Long,
                 parentId: Long,
                 creationDateTime: String,
                 score: Long,
                 viewCount: Long,
                 title: String,
                 sentiment: String,
                 body: String,
                 ownerUserId: Long,
                 tags: String,
                 answerCount: Long,
                 commentCount: Long,
                 favoriteCount: Long
               )

case class PostWithLocation (rowKey: String,
                             postId: Long,
                             postTypeId: Long,
                             parentId: Long,
                             creationDateTime: String,
                             score: Long,
                             viewCount: Long,
                             title: String,
                             sentiment: String,
                             body: String,
                             ownerUserId: Long,
                             tags: String,
                             answerCount: Long,
                             commentCount: Long,
                             favoriteCount: Long,
                             userName: String,
                             location: String
                )


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

  def replaceQuotes(text: String) = text.replaceAll("&quot;", "'")

  def savePostAsCsv(dStream: DStream[PostWithLocation], path: String): Unit = {
    var rddNumber = 1
    dStream.foreachRDD { rdd =>

      val ds = spark.createDataset(rdd)(Encoders.product[PostWithLocation])

      ds.write.csv(s"$path/$rddNumber")
      rddNumber += 1
    }
  }
}
