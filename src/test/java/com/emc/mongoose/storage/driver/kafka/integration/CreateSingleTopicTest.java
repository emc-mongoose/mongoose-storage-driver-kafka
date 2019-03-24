package com.emc.mongoose.storage.driver.kafka.integration;

import com.emc.mongoose.storage.driver.kafka.util.docker.KafkaNodeContainer;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.admin.*;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class CreateSingleTopicTest {

  private static KafkaNodeContainer KAFKA_NODE_CONTAINER;
  private static AdminClient adminClient;
  private static Properties properties = new Properties();

  @Test
  public void createTopic() throws Exception {
    final var topic = getClass().getSimpleName();
    final var topicsToCreate = Collections.singleton(new NewTopic(topic, 2, (short) 1));
    final var results = adminClient.createTopics(topicsToCreate).values();
    Assert.assertTrue(
        "Topic with specified name \"" + topic + "\" wasn't created\n", results.containsKey(topic));
  }

  @BeforeClass
  public static void setupClass() throws Exception {
    KAFKA_NODE_CONTAINER = new KafkaNodeContainer();
    properties.put(
        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
        KAFKA_NODE_CONTAINER.getContainerIp() + ":9092");
    adminClient = KafkaAdminClient.create(properties);
  }

  @AfterClass
  public static void tearDownClass() {
    KAFKA_NODE_CONTAINER.close();
  }
}
