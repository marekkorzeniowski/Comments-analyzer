package Kafka

import java.util.Properties
import scala.io.Source
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object Producer {

  def dataProducer(): Unit = {
       new Thread(
         () => {
           println("Another thread started")

           val topic = "comment-analyzer"
           val brokers = "localhost:9092"
           val path = "/home/marek/Repos/Comments-analyzer/src/main/resources/data/comments_sample.xml"

           val props = new Properties()
           props.put("bootstrap.servers", brokers)
           props.put("client.id", "ScalaProducerExample")
           props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
           props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

           val producer = new KafkaProducer[String, String](props)

           val lines = Source.fromFile(path).getLines().filter(_.startsWith("  <row")).map(_.strip())

           lines.foreach { line =>
             val data = new ProducerRecord[String, String](topic, "Comment", line)
             producer.send(data)
             Thread.sleep(1000)
           }
           producer.close()
         }).start()
  }

}
