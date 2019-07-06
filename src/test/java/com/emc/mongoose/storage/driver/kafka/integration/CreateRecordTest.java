package com.emc.mongoose.storage.driver.kafka.integration;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.*;

public class CreateRecordTest {

  private KafkaProducer<String, byte[]> kafkaProducer;
  private static final String TOPIC_NAME =
      "topic" + CreateRecordTest.class.getSimpleName() + System.currentTimeMillis();
  private static final String KEY_NAME = "key" + CreateRecordTest.class.getSimpleName();
  private static final String IP = "localhost:9092";
  private static AdminClient adminClient;

  @Before
  public void setup() {
    Properties properties = new Properties();
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, IP);
    Properties producer_properties = new Properties();
    producer_properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, IP);
    kafkaProducer =
        new KafkaProducer<>(producer_properties, new StringSerializer(), new ByteArraySerializer());
    adminClient = KafkaAdminClient.create(properties);
  }

  @Test
  public void testCreateRecord() throws Exception {
    adminClient.createTopics(Collections.singletonList(new NewTopic(TOPIC_NAME, 1, (short) 1)));
    final byte[] data = new byte[900000];
    final ProducerRecord<String, byte[]> producerRecord =
        new ProducerRecord<>(TOPIC_NAME, KEY_NAME, data);
    Future<RecordMetadata> future =
        kafkaProducer.send(
            producerRecord,
            ((metadata, exception) -> {
              Assert.assertNull(exception);
              Assert.assertNotNull(metadata);
            }));
    RecordMetadata recordMetadata = future.get(1000, TimeUnit.MILLISECONDS);
    System.out.println("Record was sent");
    Assert.assertEquals("Offset must be 0", 0, recordMetadata.offset());
    Assert.assertEquals(
        "Name of the topic must be " + TOPIC_NAME, TOPIC_NAME, recordMetadata.topic());
    Assert.assertEquals(
        "Value size must be " + data.length, recordMetadata.serializedValueSize(), 900000);
  }

  @After
  public void teardown() {
    if (kafkaProducer != null) {
      kafkaProducer.close();
    }
    if (adminClient != null) {
      adminClient.close();
    }
  }
}
