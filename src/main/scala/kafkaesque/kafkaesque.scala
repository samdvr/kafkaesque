import io.circe.{Encoder}
import io.circe.generic.auto._
import io.circe.syntax._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import java.util.Properties
import com.typesafe.config.ConfigFactory
import java.util.concurrent.Future

object Kafkaesque {

  implicit class Publisher[A](data: A) {

    def publish(topic: String)(implicit encoder: Encoder[A]) = {
      producer.send(new ProducerRecord[String, String](topic, data.asJson.noSpaces))
    }
  }

  implicit class SeqPublisher[A](dataSeq: Seq[A]) {
    def publish(topic: String)(implicit encoder: Encoder[A]): Seq[Future[RecordMetadata]] = {
      dataSeq.map( data => producer.send(new ProducerRecord[String, String](topic, data.asJson.noSpaces)))
    }
  }

  private lazy val producer = new KafkaProducer[String, String](kafkaConfig)

  private lazy val kafkaConfig = {
    val props = new Properties()
    val conf = ConfigFactory.load()
    props.put("bootstrap.servers", conf.getString("kafkaesque.brokers"))
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props

  }
  
}
