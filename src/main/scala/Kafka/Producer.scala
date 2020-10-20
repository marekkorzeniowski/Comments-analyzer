package Kafka

import java.util.Properties
import scala.io.Source
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object Producer {

  def dataProducer(): Unit = {
       new Thread(
         () => {
           println("Another thread started")

           val KAFKA_HOST = "localhost"
           val KAFKA_PORT = "9092"
           val KAFKA_TOPIC = "comment-analyzer"
           val ROW_KEY = "Comment"

           val FILE_PATH = "/home/marek/Repos/Comments-analyzer/src/main/resources/data/comments_sample.xml"



           val props = new Properties()
           props.put("bootstrap.servers", s"$KAFKA_HOST:$KAFKA_PORT")
           props.put("client.id", "ScalaProducerExample")
           props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
           props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

           val producer = new KafkaProducer[String, String](props)

           val lines = Source.fromFile(FILE_PATH).getLines().filter(_.startsWith("  <row")).map(_.strip())

           lines.foreach { line =>
             val data = new ProducerRecord[String, String](KAFKA_TOPIC, ROW_KEY, line)
             producer.send(data)
             Thread.sleep(1000)
           }
           producer.close()
           println("Second thread finished")
         }).start()
  }

}
