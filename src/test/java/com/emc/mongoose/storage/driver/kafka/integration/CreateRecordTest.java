package com.emc.mongoose.storage.driver.kafka.integration;

import java.sql.Timestamp;
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
  private static final String TOPIC_NAME = "topic";
  private static final String KEY_NAME = "key";
  private static AdminClient adminClient;
  private static Timestamp timestamp;

  @Before
  public void setup() {
    timestamp = new Timestamp(System.currentTimeMillis());
    String host_port = "127.0.0.1:9092";
    Properties properties = new Properties();
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, host_port);
    Properties producer_properties = new Properties();
    producer_properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, host_port);
    kafkaProducer =
        new KafkaProducer<>(producer_properties, new StringSerializer(), new ByteArraySerializer());
    adminClient = KafkaAdminClient.create(properties);
  }

  @Test
  public void testCreateRecord() throws Exception {
    String topicName = TOPIC_NAME + timestamp.getTime();
    adminClient.createTopics(Collections.singletonList(new NewTopic(topicName, 1, (short) 1)));
    final byte[] data = new byte[900000];
    final ProducerRecord<String, byte[]> producerRecord =
        new ProducerRecord<>(topicName, KEY_NAME, data);
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
        "Name of the topic must be " + topicName, topicName, recordMetadata.topic());
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
