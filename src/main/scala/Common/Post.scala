package Common

import NLP._
import Main.Consumer.{spark, ssc}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Dataset, Encoders}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import scala.xml.{Elem, XML}

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
                 favoriteCount: Long)

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
                             location: String)


object Post {

  def processPosts(rowKey: String ,xmlRecord: Elem, lexicon: Broadcast[Map[String, Int]]): Post = {

    val postId = xmlRecord.attribute("Id").getOrElse(-1).toString.toLong
    val postTypeId = xmlRecord.attribute("PostTypeId").getOrElse(-1).toString.toInt
    val parentId = xmlRecord.attribute("ParentID").getOrElse(-1).toString.toInt
    val creationDateTime = xmlRecord.attribute("CreationDate").getOrElse("0001-01-01T00:00:00.000").toString
    val score = xmlRecord.attribute("Score").getOrElse(-1).toString.toInt
    val viewCount = xmlRecord.attribute("ViewCount").getOrElse(-1).toString.toLong
    val title = xmlRecord.attribute("Title").getOrElse("N/A").toString
    val parsedTitle = Common.parseTitle(title)
    val body = xmlRecord.attribute("Body").getOrElse("N/A").toString
    val parsedBody = Common.parseBody(body)
    val tokenizer = SentimentAnalysis.tokenizer(parsedBody, lexicon)
    val avgScore = SentimentAnalysis.calculateAvgScore(tokenizer)
    val sentiment = SentimentType.fromScore(avgScore).toString
    val ownerUserId = xmlRecord.attribute("OwnerUserId").getOrElse(-1).toString.toLong
    val tags = xmlRecord.attribute("Tags").getOrElse("N/A").toString
    val parsedTags = Common.parseTags(tags)
    val answerCount = xmlRecord.attribute("AnswerCount").getOrElse(-1).toString.toInt
    val commentCount = xmlRecord.attribute("CommentCount").getOrElse(-1).toString.toInt
    val favoriteCount = xmlRecord.attribute("FavoriteCount").getOrElse(-1).toString.toInt


    Post(rowKey, postId, postTypeId, parentId, creationDateTime, score,
        viewCount, parsedTitle, sentiment, parsedBody, ownerUserId,
        parsedTags, answerCount, commentCount, favoriteCount)

  }

  def readFromKafkaPosts(topic: String, kafkaParams: Map[String, Object], lexicon: Broadcast[Map[String, Int]]): DStream[Post] = {
    val topics = Array(topic)
    val kafkaDStream = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent, //Distributes the partitions evenly across the Spark cluster
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams + ("group.id" -> "group2"))
    )

    val processedStream = kafkaDStream.map { record =>

      val xml = XML.loadString(record.value())
      processPosts(record.key(), xml, lexicon)
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

  def savePostAsCsv(dStream: DStream[PostWithLocation], path: String): Unit = {

    dStream.foreachRDD { rdd =>

      val ds = spark.createDataset(rdd)(Encoders.product[PostWithLocation])

      val timeStamp = System.currentTimeMillis()
      ds.write.csv(s"$path/$timeStamp")
    }
  }
}
