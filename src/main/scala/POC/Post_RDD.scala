package POC

import Common.Post
import Common.Post.{parseBody, parseTags, replaceQuotes}
import org.apache.spark.sql.SparkSession

import scala.io.Source
import scala.xml.{Elem, XML}

object Post_RDD extends App {
  val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("posts-processing")
    .getOrCreate()

  val path_words = "src/main/scala/POC/positive_and_negative_words.txt"
  val pos_neg_words = Source.fromFile(path_words)     //TODO - Tutaj chyba lepiej wczytać jako RDD?
    .getLines().toVector.map(_.split(","))
    .map(array => (array(0), array(1).toInt)).toMap


  def tokenizer(text: String): Array[(String, Int)] = {
    text.split("\\s+").map(word =>
      word.replaceAll("\\W", "").replaceAll("\\d", "").toLowerCase)
      .filter(_.length != 0)
      .map {
        case token if pos_neg_words.contains(token) => (token, pos_neg_words(token))
        case token => (token, 0)
      }
  }

  def calculateAvgScore(tokens: Array[(String, Int)]) = {
    val score = tokens.foldLeft(0.0) { (acc, tuple) => acc + tuple._2 }

    score / tokens.length
  }

  def processPostsFromRdd(xmlRecord: Elem): Post = {

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

    val tokens = tokenizer(parsedBody)
    val avg_score = calculateAvgScore(tokens)

    val sentiment = SentimentTypeLex.fromScore(avg_score).toString
    val ownerUserId = xmlRecord.attribute("OwnerUserId").getOrElse(-1).toString.toLong
    val tags = xmlRecord.attribute("Tags").getOrElse("N/A").toString
    val parsedTags = parseTags(tags)
    val answerCount = xmlRecord.attribute("AnswerCount").getOrElse(-1).toString.toInt
    val commentCount = xmlRecord.attribute("CommentCount").getOrElse(-1).toString.toInt
    val favoriteCount = xmlRecord.attribute("FavoriteCount").getOrElse(-1).toString.toInt


    Post("Post", postId, postTypeId, parentId, creationDateTime, score,
      viewCount, parsedTitle, sentiment, parsedBody, ownerUserId,
      parsedTags, answerCount, commentCount, favoriteCount)
  }


  val t0 = System.nanoTime()

  val path = "src/main/resources/data/Posts.xml"
  val posts_text_rdd = spark.sparkContext.textFile(path).filter(_.startsWith("  <row"))
    .map(rdd => XML.loadString(rdd))
    .map(xml => processPostsFromRdd(xml))

  import spark.implicits._

  val posts_DS = spark.createDataset(posts_text_rdd)


  posts_DS.write.csv("src/main/resources/processed_posts_full")

  val t1 = System.nanoTime()
  println(s"Total time of processing: ${(t1 - t0) / 1e9}s")


}
