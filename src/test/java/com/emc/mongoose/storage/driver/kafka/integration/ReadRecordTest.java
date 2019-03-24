package com.emc.mongoose.storage.driver.kafka.integration;

import com.emc.mongoose.storage.driver.kafka.util.docker.KafkaNodeContainer;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.*;

public class ReadRecordTest {
  private static KafkaNodeContainer kafkaNodeContainer;
  private MockConsumer<String, String> mockConsumer;
  private static final String TOPIC_NAME = "test-topic";
  private static final String KEY_NAME = "key";
  private static final String DATA = "test-record";
  private static final Duration TIMEOUT = Duration.ZERO;

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
    mockConsumer = new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST);
  }

  @After
  public void closeMockConsumer() {
    if (mockConsumer != null && !mockConsumer.closed()) {
      mockConsumer.close();
    }
  }

  @Test
  public void readRecordTest() throws Exception {
    TopicPartition topicPartition = new TopicPartition(TOPIC_NAME, 0);
    mockConsumer.assign(Arrays.asList(topicPartition));

    HashMap<TopicPartition, Long> beginningOffsets = new HashMap<>();
    beginningOffsets.put(new TopicPartition(TOPIC_NAME, 0), 0L);
    mockConsumer.updateBeginningOffsets(beginningOffsets);

    ConsumerRecord<String, String> record =
        new ConsumerRecord<String, String>(TOPIC_NAME, 0, 0L, KEY_NAME, DATA);
    mockConsumer.addRecord(record);

    Assert.assertEquals(
        "Record must be " + record.toString(),
        record,
        mockConsumer.poll(TIMEOUT).records(topicPartition).get(0));
  }
}
