package Main

import Common._
import Kafka.Producer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Consumer {

  val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")     // TODO - remove it in remote version
    .appName("comment-analyzer")
    .getOrCreate()

  val ssc = new StreamingContext(spark.sparkContext, Seconds(5))


  def main(args: Array[String]): Unit = {

//        //1 TODO - replace parameters below with args
//        if (args.length != 7) {
//          println(
//            """
//              |Required parameters:
//              |1) Kafka host
//              |2) Topic 1
//              |3) Topic 2
//              |4) Users path
//              |5) Lexicon path
//              |6) Output for Comments
//              |7) Output for Posts
//              |""".stripMargin)
//          System.exit(1)
//        }

    val KAFKA_HOST =  "localhost" // args(0) //
    val KAFKA_PORT = "9092"
    val KAFKA_TOPIC1 = "comment-analyzer"  // args(1)  //
    val KAFKA_TOPIC2 = "post-analyzer"    // args(2)   //
    val USERS_DF = "src/main/resources/data/users_parquet"  // args(3)  //
    val LEXICON = "src/main/resources/data/positive_and_negative_words.txt" //args(4)   //
    val OUTPUT1 = "/home/marek/Repos/Comments-analyzer/src/main/resources/comments_csv" //args(5)   //
    val OUTPUT2 =  "/home/marek/Repos/Comments-analyzer/src/main/resources/posts_csv" //args(6) //


    val KAFKA_PARAMS: Map[String, Object] = Map(
      "bootstrap.servers" -> s"$KAFKA_HOST:$KAFKA_PORT",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> false.asInstanceOf[Object]
    )

    println("Processing about to start")
    val t0 = System.nanoTime()

    import spark.implicits._
    val users = spark.read.parquet(USERS_DF).as[(Long, User)]
    val lexicon = NLP.SentimentAnalysis.readWords(LEXICON)

    Producer.dataProducer()                   // TODO - remove in remote version


    val dStream1 = Comment.readFromKafkaComments(KAFKA_TOPIC1, KAFKA_PARAMS, lexicon)
    val joinedDStream1 = Comment.joinCommentsWithStaticData(dStream1, users)
//    joinedDStream1.print(1000)

    Comment.saveCommentAsCsv(joinedDStream1, OUTPUT1)
    val commentsPathParquet = "/home/marek/Repos/Comments-analyzer/src/main/resources/comments_parquet"
    Comment.saveCommentAsParquet(joinedDStream1, commentsPathParquet)

    val dStream2 = Post.readFromKafkaPosts(KAFKA_TOPIC2, KAFKA_PARAMS, lexicon)
    val joinedDStream2 = Post.joinPostsWithStaticData(dStream2, users)
//    joinedDStream2.print(1000)

    Post.savePostAsCsv(joinedDStream2, OUTPUT2)
    val postsPathParquet = "/home/marek/Repos/Comments-analyzer/src/main/resources/posts_parquet"
    Post.savePostAsParquet(joinedDStream2, postsPathParquet)

    ssc.start()
    ssc.awaitTermination()
    val t1 = System.nanoTime()

    println(s"Total time of processing: ${(t1-t0)/1e9}s")

  }
}

