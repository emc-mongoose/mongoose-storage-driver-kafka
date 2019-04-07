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

  private static final String topicName = "test-topic-for-delete";

  @Test
  public void deleteTopic() throws Exception {
    DeleteTopicsResult result =
        adminClient.deleteTopics(Collections.singleton(topicName));
    Assert.assertTrue(
        "Topic with specified name \"" + topicName + "\" wasn't deleted\n",
        result.values().containsKey(topicName));
  }

  @BeforeClass
  public static void setupClass() throws Exception {
    KAFKA_NODE_CONTAINER = new KafkaNodeContainer();
    properties.put(
        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
        KAFKA_NODE_CONTAINER.getContainerIp() + ":9092");
    adminClient = KafkaAdminClient.create(properties);
    final CreateTopicsResult result =
    adminClient.createTopics(Collections.singleton(new NewTopic(topicName, 2, (short) 1)));
}

  @AfterClass
  public static void tearDownClass() {
    KAFKA_NODE_CONTAINER.close();
  }
}
