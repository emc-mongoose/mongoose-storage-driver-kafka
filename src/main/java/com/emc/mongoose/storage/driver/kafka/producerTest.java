package com.emc.mongoose.storage.driver.kafka;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class producerTest {
  private static final String TOPIC_NAME = "topic";
  private static final String KEY_NAME = "key";

  public static void main(String[] args) {

    final Properties prodProps = new Properties();
    prodProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    prodProps.put(ProducerConfig.ACKS_CONFIG, "all");
    prodProps.put(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");
    prodProps.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");
    final Producer<String, String> prod = new KafkaProducer<>(prodProps);

    final ProducerRecord<String, String> producerRecord =
        new ProducerRecord<>(TOPIC_NAME, KEY_NAME, "data");
    try {
      prod.send(producerRecord).get();
      prod.send(producerRecord).get();
      prod.send(producerRecord).get();
      prod.send(producerRecord).get();
    } catch (Exception e) {
      System.out.println("error");
    }
    prod.flush();
    prod.close();
  }
}
