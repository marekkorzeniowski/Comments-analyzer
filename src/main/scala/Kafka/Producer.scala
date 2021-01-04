package Kafka

import java.util.Properties
import scala.io.Source
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object Producer {

  val BREAK_TIME = 1

  def dataProducer(): Unit = { // Used only for testing purposes
       new Thread(
         () => {
           println("Second thread started")

           val KAFKA_HOST = "localhost"
           val KAFKA_PORT = "9092"
           val KAFKA_TOPIC = "comment-analyzer"
           val ROW_KEY = "Comment"

           val FILE_PATH1 = "src/main/resources/data/Comments.xml"



           val props = new Properties()
           props.put("bootstrap.servers", s"$KAFKA_HOST:$KAFKA_PORT")
           props.put("client.id", "Producer_1")
           props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
           props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

           val producer = new KafkaProducer[String, String](props)

           val lines = Source.fromFile(FILE_PATH1).getLines().filter(_.startsWith("  <row")).map(_.strip())

           lines.foreach { line =>
             val data = new ProducerRecord[String, String](KAFKA_TOPIC, ROW_KEY, line)
             producer.send(data)
             Thread.sleep(BREAK_TIME)
           }

           Thread.sleep(30000)
           producer.close()
           println("Second thread finished")
         }).start()

    new Thread(
      () => {
        println("Third thread started")

        val KAFKA_HOST = "localhost"
        val KAFKA_PORT = "9092"
        val KAFKA_TOPIC = "post-analyzer"
        val ROW_KEY = "Post"

        val FILE_PATH2 = "src/main/resources/data/Posts.xml"



        val props = new Properties()
        props.put("bootstrap.servers", s"$KAFKA_HOST:$KAFKA_PORT")
        props.put("client.id", "Producer_2")
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

        val producer = new KafkaProducer[String, String](props)

        val lines = Source.fromFile(FILE_PATH2).getLines().filter(_.startsWith("  <row")).map(_.strip())

        lines.foreach { line =>
          val data = new ProducerRecord[String, String](KAFKA_TOPIC, ROW_KEY, line)
          producer.send(data)
          Thread.sleep(BREAK_TIME)
        }

        Thread.sleep(30000)
        producer.close()
        println("Third thread finished")
      }).start()
  }
}
