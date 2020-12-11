package Main

import Common._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Consumer {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("comment-analyzer")
    .getOrCreate()

  val ssc = new StreamingContext(spark.sparkContext, Seconds(5))


  def main(args: Array[String]): Unit = {

        if (args.length != 7) {
          println(
            """
              |Required parameters:
              |1) Kafka host
              |2) Topic 1
              |3) Topic 2
              |4) Users path
              |5) Lexicon path
              |6) Output for Comments
              |7) Output for Posts
              |""".stripMargin)
          System.exit(1)
        }

    val KAFKA_HOST = args(0)
    val KAFKA_PORT = "9092"
    val KAFKA_TOPIC1 = args(1)
    val KAFKA_TOPIC2 =args(2)
    val USERS_DF = args(3)
    val LEXICON = args(4)
    val OUTPUT1 = args(5)
    val OUTPUT2 =  args(6)


    val KAFKA_PARAMS: Map[String, Object] = Map(
      "bootstrap.servers" -> s"$KAFKA_HOST:$KAFKA_PORT",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> false.asInstanceOf[Object]
    )

    println("Processing about to start")

    import spark.implicits._
    val users = spark.read.parquet(USERS_DF).as[(Long, User)]
    val lexicon = NLP.SentimentAnalysis.readWords(LEXICON)


    val dStream1 = Comment.readFromKafkaComments(KAFKA_TOPIC1, KAFKA_PARAMS, lexicon)
    val joinedDStream1 = Comment.joinCommentsWithStaticData(dStream1, users)

    Comment.saveCommentAsParquet(joinedDStream1, OUTPUT1)


    val dStream2 = Post.readFromKafkaPosts(KAFKA_TOPIC2, KAFKA_PARAMS, lexicon)
    val joinedDStream2 = Post.joinPostsWithStaticData(dStream2, users)

    Post.savePostAsParquet(joinedDStream2, OUTPUT2)

    ssc.start()
    ssc.awaitTermination()
  }
}

