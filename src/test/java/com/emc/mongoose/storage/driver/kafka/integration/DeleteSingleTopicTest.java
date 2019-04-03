package com.emc.mongoose.storage.driver.kafka.integration;

import com.emc.mongoose.storage.driver.kafka.util.docker.KafkaNodeContainer;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.admin.*;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class DeleteSingleTopicTest {

  private static KafkaNodeContainer KAFKA_NODE_CONTAINER;
  private static AdminClient adminClient;
  private static Properties properties = new Properties();

  @Test
  public void deleteTopic() throws Exception {
    DeleteTopicsResult result =
        adminClient.deleteTopics(Collections.singleton("test-topic"));
    Assert.assertTrue(
        "Topic with specified name \"test-topic\" wasn't deleted\n",
        result.values().containsKey("test-topic"));
  }

  @BeforeClass
  public static void setupClass() throws Exception {
    KAFKA_NODE_CONTAINER = new KafkaNodeContainer();
    properties.put(
        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
        KAFKA_NODE_CONTAINER.getContainerIp() + ":9092");
    adminClient = KafkaAdminClient.create(properties);
    CreateSingleTopicTest createTopic = new CreateSingleTopicTest();
    createTopic.createTopic();
  }

  @AfterClass
  public static void tearDownClass() {
    KAFKA_NODE_CONTAINER.close();
  }
}
