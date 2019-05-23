package com.emc.mongoose.storage.driver.kafka.integration;

import com.emc.mongoose.storage.driver.kafka.util.docker.KafkaNodeContainer;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.*;

public class CreateRecordTest {

  private static KafkaNodeContainer KAFKA_NODE_CONTAINER;
  private KafkaProducer<String, byte[]> kafkaProducer;
  private static final String TOPIC_NAME = "topic";
  private static final String KEY_NAME = "key";
  private static AdminClient adminClient;

  @BeforeClass
  public static void createContainers() {
    try {
      KAFKA_NODE_CONTAINER = new KafkaNodeContainer();
    } catch (final Exception e) {
      throw new AssertionError(e);
    }
  }

  @Before
  public void setup() {
    String host_port = KAFKA_NODE_CONTAINER.getKafkaIp();
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
    adminClient.createTopics(Collections.singletonList(new NewTopic(TOPIC_NAME, 1, (short) 1)));
    final byte[] one_mb_of_data = new byte[900000];
    final ProducerRecord<String, byte[]> producerRecord =
      new ProducerRecord<>(TOPIC_NAME, KEY_NAME, one_mb_of_data);
    Future<RecordMetadata> future = kafkaProducer.send(
      producerRecord, ((metadata, exception) -> {
        Assert.assertNull(exception);
        Assert.assertNotNull(metadata);
      })
    );
    RecordMetadata recordMetadata = future.get(1000, TimeUnit.MILLISECONDS);
    System.out.println("Record was sent");
    Assert.assertEquals("Offset must be 0", 0, recordMetadata.offset());
    Assert.assertEquals(
      "Name of the topic must be " + TOPIC_NAME, TOPIC_NAME,recordMetadata.topic());
    Assert.assertEquals("Value size must be 1 mb", recordMetadata.serializedValueSize(), 900000);
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

  @AfterClass
  public static void tearDownClass() {
    KAFKA_NODE_CONTAINER.close();
  }
}
