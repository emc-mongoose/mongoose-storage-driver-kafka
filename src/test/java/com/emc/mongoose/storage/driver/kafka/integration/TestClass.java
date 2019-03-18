package com.emc.mongoose.storage.driver.kafka.integration;

import com.emc.mongoose.storage.driver.kafka.util.docker.KafkaNodeContainer;
import org.junit.AfterClass;
import org.junit.Test;

public class TestClass {

  private static KafkaNodeContainer KAFKA_NODE_CONTAINER;

  public TestClass() throws Exception {}

  @Test
  public void createContainers() throws Exception {
    try {
      KAFKA_NODE_CONTAINER = new KafkaNodeContainer();
    } catch (final Exception e) {
      throw new AssertionError(e);
    }
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    KAFKA_NODE_CONTAINER.close();
  }
}
