package io.kafkaesque.smoke;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Classic kafka-clients smoke: produce → consume with a consumer group →
 * commit offsets. Honors ApiVersions negotiation; does not enable
 * group.instance.id, transactions, or KIP-848.
 */
public final class SmokeMain {
  public static void main(String[] args) throws Exception {
    String bootstrap = System.getenv().getOrDefault("KAFKAESQUE_BOOTSTRAP", "127.0.0.1:9092");
    String topic = "java-smoke-" + UUID.randomUUID().toString().substring(0, 8);
    String group = "java-smoke-group-" + UUID.randomUUID().toString().substring(0, 8);
    String payload = "hello-from-java-clients";

    Properties producerProps = new Properties();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
    producerProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "15000");

    try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
      producer.send(new ProducerRecord<>(topic, "k1", payload)).get(15, TimeUnit.SECONDS);
      producer.flush();
    }

    Properties consumerProps = new Properties();
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, group);
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    // Classic assignor path — no static membership.
    consumerProps.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
        "org.apache.kafka.clients.consumer.RangeAssignor");

    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
      consumer.subscribe(Collections.singletonList(topic));
      long deadline = System.currentTimeMillis() + 30_000;
      ConsumerRecord<String, String> got = null;
      while (System.currentTimeMillis() < deadline && got == null) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
        for (ConsumerRecord<String, String> r : records) {
          got = r;
          break;
        }
      }
      if (got == null) {
        throw new IllegalStateException("timed out waiting for produced record on " + topic);
      }
      if (!payload.equals(got.value())) {
        throw new IllegalStateException("payload mismatch: " + got.value());
      }
      TopicPartition tp = new TopicPartition(got.topic(), got.partition());
      consumer.commitSync(Collections.singletonMap(tp, new OffsetAndMetadata(got.offset() + 1)));
      OffsetAndMetadata committed = consumer.committed(Collections.singleton(tp)).get(tp);
      if (committed == null || committed.offset() != got.offset() + 1) {
        throw new IllegalStateException("offset commit did not stick: " + committed);
      }
      System.out.println("java-smoke ok topic=" + topic + " group=" + group
          + " offset=" + got.offset() + " committed=" + committed.offset());
    }
  }

  private SmokeMain() {}
}
