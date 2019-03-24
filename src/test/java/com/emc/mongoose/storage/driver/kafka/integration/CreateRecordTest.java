package com.emc.mongoose.storage.driver.kafka.integration;

import java.util.Collections;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.*;

public class CreateRecordTest {

  private MockProducer<String, String> mockProducer;
  private static final int NUMBER_OF_ELEMENTS = 500000;
  private static final String TOPIC_NAME = "topic" + CreateRecordTest.class.getSimpleName();
  private static final String KEY_NAME = "key" + CreateRecordTest.class.getSimpleName();

  @Before
  public void setup() {
    final StringSerializer stringSerializer = new StringSerializer();
    mockProducer = new MockProducer<>(true, stringSerializer, stringSerializer);
  }

  @Test
  public void testCreateRecord() throws Exception {
    final String data = new String(new char[NUMBER_OF_ELEMENTS]);
    final ProducerRecord<String, String> producerRecord =
        new ProducerRecord<>(TOPIC_NAME, KEY_NAME, data);
    final Future<RecordMetadata> recordMetadataFuture =
        mockProducer.send(
            producerRecord,
            (recordMetaData, exception) -> System.out.println("The record was sent"));
    Assert.assertEquals(
        "Record must be in history",
        Collections.singletonList(producerRecord),
        mockProducer.history());
    final var recordMetadata = recordMetadataFuture.get();
    Assert.assertTrue("Send must be done", recordMetadataFuture.isDone());
    Assert.assertEquals(
        "Name of the topic must be " + TOPIC_NAME, TOPIC_NAME, recordMetadata.topic());
    Assert.assertEquals("Offset must be 0", 0, recordMetadata.offset());
  }

  @After
  public void teardown() {
    if (mockProducer != null && !mockProducer.closed()) {
      mockProducer.close();
    }
  }
}
