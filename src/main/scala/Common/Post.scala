package Common

import NLP._
import Main.Consumer.{spark, ssc}
import NLP.SentimentAnalysis.{getScore, tokenizer}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Dataset, Encoders}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import java.sql.Timestamp
import scala.util.Try
import scala.xml.{Elem, XML}

case class Post (rowKey: String,
                 postId: Long,
                 postTypeId: Long,
                 parentId: Long,
                 creationDateTime: Timestamp,
                 score: Long,
                 viewCount: Long,
                 title: String,
                 sentiment_label: String,
                 avg_score: Float,
                 pos_score: Int,
                 neg_score:Int,
                 tot_score: Int,
                 body: String,
                 ownerUserId: Long,
                 tags: String,
                 answerCount: Long,
                 commentCount: Long,
                 favoriteCount: Long)

case class PostWithLocation (rowKey: String,
                             postId: Long,
                             postTypeId: Long,
                             parentId: Long,
                             creationDateTime: Timestamp,
                             score: Float,
                             viewCount: Long,
                             title: String,
                             sentiment_label: String,
                             avg_score: Float,
                             pos_score: Int,
                             neg_score:Int,
                             tot_score: Int,
                             body: String,
                             ownerUserId: Long,
                             tags: String,
                             answerCount: Long,
                             commentCount: Long,
                             favoriteCount: Long,
                             userName: String,
                             location: String)


object Post {

  def processPosts(xmlRecord: Elem, pos_words: Broadcast[Map[String, Int]],
                   neg_words: Broadcast[Map[String, Int]]): Post = {

    val postId = xmlRecord.attribute("Id").getOrElse(-1).toString.toLong
    val postTypeId = xmlRecord.attribute("PostTypeId").getOrElse(-1).toString.toInt
    val parentId = xmlRecord.attribute("ParentID").getOrElse(-1).toString.toInt
    val creationDateTime = xmlRecord.attribute("CreationDate").getOrElse("0001-01-01 00:00:00.000").toString
    val timestamp = Common.getTimeStampFromString(creationDateTime)
    val score = xmlRecord.attribute("Score").getOrElse(-1).toString.toInt
    val viewCount = xmlRecord.attribute("ViewCount").getOrElse(-1).toString.toLong
    val title = xmlRecord.attribute("Title").getOrElse("N/A").toString
    val parsedTitle = Common.parseTitle(title)
    val body = xmlRecord.attribute("Body").getOrElse("N/A").toString
    val parsedBody = Common.parseBody(body)
    val pos_tokens = tokenizer(parsedBody, pos_words)
    val pos_score = getScore(pos_tokens)
    val neg_tokens = tokenizer(parsedBody, neg_words)
    val neg_score = getScore(neg_tokens)
    val tot_score = pos_score - neg_score
    val avg_score = Try(tot_score.toFloat/pos_tokens.length).getOrElse(0f)
    val sentiment_label = SentimentType.fromScore(avg_score).toString
    val ownerUserId = xmlRecord.attribute("OwnerUserId").getOrElse(-1).toString.toLong
    val tags = xmlRecord.attribute("Tags").getOrElse("N/A").toString
    val parsedTags = Common.parseTags(tags)
    val answerCount = xmlRecord.attribute("AnswerCount").getOrElse(-1).toString.toInt
    val commentCount = xmlRecord.attribute("CommentCount").getOrElse(-1).toString.toInt
    val favoriteCount = xmlRecord.attribute("FavoriteCount").getOrElse(-1).toString.toInt


    Post("Post", postId, postTypeId, parentId, timestamp, score,
      viewCount, parsedTitle,
      sentiment_label, avg_score, pos_score, neg_score, tot_score,
      parsedBody, ownerUserId,
      parsedTags, answerCount, commentCount, favoriteCount)

  }

  def readFromKafkaPosts(topic: String, kafkaParams: Map[String, Object],
                         pos_words: Broadcast[Map[String, Int]],
                         neg_words: Broadcast[Map[String, Int]]): DStream[Post] = {

    val topics = Array(topic)
    val kafkaDStream = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent, //Distributes the partitions evenly across the Spark cluster
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams + ("group.id" -> "group2"))
    )

    val processedStream = kafkaDStream.map { record =>
      val xml = XML.loadString(record.value())
      processPosts(xml, pos_words, neg_words)
    }
    processedStream
  }

  def joinPostsWithStaticData(dStream: DStream[Post], users: Dataset[(Long, User)]): DStream[PostWithLocation] = {

    val usersRDD = users.rdd
    val dStreamTuple = dStream.map(post => (post.ownerUserId, post))

    val joinedDStream = dStreamTuple.transform(stream => stream.leftOuterJoin(usersRDD))

    joinedDStream.map{case (_, (post, user)) =>
      PostWithLocation(post.rowKey,
        post.postId,
        post.postTypeId,
        post.parentId,
        post.creationDateTime,
        post.score,
        post.viewCount,
        post.title,
        post.sentiment_label,
        post.avg_score,
        post.pos_score,
        post.neg_score,
        post.tot_score,
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

  def savePostAsCsv(dStream: DStream[PostWithLocation], path: String): Unit = {
    dStream.foreachRDD { rdd =>
      val ds = spark.createDataset(rdd)(Encoders.product[PostWithLocation])

      val timeStamp = Common.getCurrentTimeStamp
      ds.write.option("header", value = true).option("delimiter", "@#$").csv(s"$path/dt=$timeStamp")
    }
  }

  def savePostAsParquet(dStream: DStream[PostWithLocation], path: String): Unit = {
    dStream.foreachRDD { rdd =>
      val ds = spark.createDataset(rdd)(Encoders.product[PostWithLocation])

      val timeStamp = Common.getCurrentTimeStamp
      ds.write.parquet(s"$path/dt=$timeStamp")
    }
  }
}
