
import java.util.Properties

import com.typesafe.config.ConfigFactory
import kafkaesque.data.Event
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object Kafkaesque {

  def kafkaConfig = {
    val props = new Properties()
    val conf = ConfigFactory.load()
    props.put("bootstrap.servers", conf.getString("kafkaesque.brokers"))
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props

  }

  implicit class Publisher(events: Seq[Event]) {
    def publish = {
      val producer = new KafkaProducer[String, String](kafkaConfig)

      val producedEvents = events.map {
        event => producer.send(new ProducerRecord[String, String](event.topic, event.message))
      }

      producer.close()
      producedEvents
    }
  }
}
