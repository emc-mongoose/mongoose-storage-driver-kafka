package com.emc.mongoose.storage.driver.kafka.integration;

import com.emc.mongoose.storage.driver.kafka.util.docker.KafkaNodeContainer;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.junit.*;

public class ReadRecordTest {
  private static KafkaNodeContainer kafkaNodeContainer;
  private Consumer<String, String> consumer;
  private Producer<String, String> producer;
  private static final String TOPIC_NAME = "topic" + ReadRecordTest.class.getSimpleName();
  private static final String KEY_NAME = "key" + ReadRecordTest.class.getSimpleName();
  private static final String DATA = "test-record";
  private static final Duration TIMEOUT = Duration.ZERO;
  private static Properties props;

  @BeforeClass
  public static void initContainer() {
    try {
      kafkaNodeContainer = new KafkaNodeContainer();
    } catch (final Exception e) {
      throw new AssertionError(e);
    }
  }

  @AfterClass
  public static void closeContainer() {
    kafkaNodeContainer.close();
  }

  @Before
  public void initMockConsumer() {
    props = new Properties();
    props.put(
        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaNodeContainer.getContainerIp() + ":9092");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("group.id", "null");
    consumer = new KafkaConsumer<>(props);
    producer = new KafkaProducer<>(props);
  }

  @After
  public void closeMockConsumer() {
    if (consumer != null) {
      consumer.close();
    }
    if (producer != null) {
      producer.close();
    }
  }

  @Test
  public void readRecordTest() throws Exception {
    consumer.subscribe(Arrays.asList(TOPIC_NAME));
    final ProducerRecord<String, String> producerRecord =
        new ProducerRecord<>(TOPIC_NAME, KEY_NAME, DATA);
    producer.send(producerRecord);

    final ConsumerRecords<String, String> recordsRead = consumer.poll(TIMEOUT);
    for (ConsumerRecord<String, String> consumerRecordRead : recordsRead) {
      Assert.assertEquals(
          "Record value must be " + producerRecord.value(),
          producerRecord.value(),
          consumerRecordRead.value());
      if (recordsRead.iterator().hasNext()) {
        break;
      }
    }
  }
}
