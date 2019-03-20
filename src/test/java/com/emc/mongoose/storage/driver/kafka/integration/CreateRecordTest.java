package com.emc.mongoose.storage.driver.kafka.integration;

import com.emc.mongoose.storage.driver.kafka.util.docker.KafkaNodeContainer;
import java.util.Collections;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.*;

public class CreateRecordTest {

  private static KafkaNodeContainer KAFKA_NODE_CONTAINER;
  private MockProducer<String, String> mockProducer;
  private static final int NUMBER_OF_ELEMENTS = 500000;

  @BeforeClass
  public static void createContainers() throws Exception {
    try {
      KAFKA_NODE_CONTAINER = new KafkaNodeContainer();
    } catch (final Exception e) {
      throw new AssertionError(e);
    }
  }

  @Before
  public void setup() {
    final StringSerializer stringSerializer = new StringSerializer();
    mockProducer = new MockProducer<>(true, stringSerializer, stringSerializer);
  }

  @Test
  public void testCreateRecord() {
    final String data = new String(new char[NUMBER_OF_ELEMENTS]);
    final ProducerRecord<String, String> producerRecord =
        new ProducerRecord<>("topic", "key", data);
    try {
      mockProducer.initTransactions();
      mockProducer.beginTransaction();
      mockProducer.send(
          producerRecord,
          (recordMetaData, exception) -> System.out.println("Offset = " + recordMetaData.offset()));
      mockProducer.commitTransaction();
    } catch (Exception e) {
      mockProducer.abortTransaction();
    }
    Assert.assertEquals(
        "Record must be in history",
        Collections.singletonList(producerRecord),
        mockProducer.history());
  }

  @After
  public void teardown() {
    if (mockProducer != null && !mockProducer.closed()) {
      mockProducer.close();
    }
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    KAFKA_NODE_CONTAINER.close();
  }
}
