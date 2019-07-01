package com.emc.mongoose.storage.driver.kafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import lombok.val;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class consumerTest {
  private static final String TOPIC_NAME = "topic";
  private static final String KEY_NAME = "key";

  public static void main(String[] args) {

    final Properties consProps = new Properties();
    consProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    consProps.put(ConsumerConfig.GROUP_ID_CONFIG, "testGroup");
    consProps.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
    consProps.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringDeserializer");
    consProps.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringDeserializer");
    final Consumer<String, String> cons = new KafkaConsumer<>(consProps);

    cons.subscribe(Arrays.asList(TOPIC_NAME));

    val records = cons.poll(Duration.ofSeconds(5));
    var amountOfReadRecords = 0;
    for (var record : records) {
      System.out.println(record.value());
      amountOfReadRecords += 1;
    }
    System.out.println(amountOfReadRecords);
  }
}
