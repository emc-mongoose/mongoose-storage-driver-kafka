package com.emc.mongoose.storage.driver.kafka.integration;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import lombok.val;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.*;

public class ReadRecordTest {
  private Consumer<String, String> consumer;
  private Producer<String, String> producer;
  private static AdminClient adm;
  private static Properties prodProps = new Properties();
  private static Properties consProps = new Properties();
  private static Properties admProps = new Properties();

  private static final String TOPIC_NAME =
      "topic" + ReadRecordTest.class.getSimpleName() + System.currentTimeMillis();
  private static final String KEY_NAME = "key" + ReadRecordTest.class.getSimpleName();
  private static final String DATA = "test-record";
  private static final String IP = "localhost:9092";
  private static final Duration TIMEOUT = Duration.ofMillis(1000*8);
  private static final Duration TIMEOUT_ZERO = Duration.ZERO;

  @Before
  public void init() {
    prodProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, IP);
    prodProps.put(ProducerConfig.CLIENT_ID_CONFIG, "DemoProducer");
    prodProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 50000);
    prodProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    prodProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producer = new KafkaProducer<>(prodProps);

    admProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, IP);
    adm = KafkaAdminClient.create(admProps);
    adm.createTopics(Collections.singletonList(new NewTopic(TOPIC_NAME, 1, (short) 1)));

    consProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, IP);
    consProps.put(ConsumerConfig.GROUP_ID_CONFIG, "DemoConsumer");
    consProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    consProps.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
    consProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
    consProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    consProps.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    consumer = new KafkaConsumer<>(consProps);
  }

  @Test
  public void readRecordTest() throws Exception {
    consumer.subscribe(singletonList(TOPIC_NAME));
    consumer.poll(TIMEOUT).records(TOPIC_NAME);
    val producerRecord = new ProducerRecord<>(TOPIC_NAME, KEY_NAME, DATA);
    val offset = new AtomicLong(-1);
    producer.send(
        producerRecord,
        (metadata, exception) -> {
          assertNull(exception);
          offset.set(metadata.offset());
        });
    producer.flush();
    assertTrue(offset.get() >= 0);
    val recordsRead = consumer.poll(TIMEOUT_ZERO).records(TOPIC_NAME);
    var found = false;
    for (val rec : recordsRead) {
      if (rec.offset() == offset.get()) {
        found = true;
        break;
      }
    }
    assertTrue(found);
  }

  @After
  public void close() {
    if (consumer != null) {
      consumer.close();
    }
    if (producer != null) {
      producer.close();
    }
  }
}
