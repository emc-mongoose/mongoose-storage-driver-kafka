package com.emc.mongoose.storage.driver.kafka.integration;

import com.emc.mongoose.storage.driver.kafka.util.docker.KafkaNodeContainer;
import org.apache.kafka.clients.admin.*;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.Properties;

public class CreateSingleTopicTest {

  private static KafkaNodeContainer KAFKA_NODE_CONTAINER;
  private static AdminClient adminClient;
  private static Properties properties = new Properties();

  @Test
  public void createTopic() throws Exception {
    adminClient.createTopics(Collections.singleton(new NewTopic("test-topic", 2, (short) 1)));
    Assert.assertTrue("Topic with specified name \"test-topic\" wasn't created\n", adminClient.listTopics().names().get().contains("test-topic"));
  }

  @BeforeClass
  public static void setupClass() throws Exception {
    KAFKA_NODE_CONTAINER = new KafkaNodeContainer();
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_NODE_CONTAINER.getContainerIp() + ":9092");
    adminClient = KafkaAdminClient.create(properties);
  }

  @AfterClass
  public static void tearDownClass() {
    KAFKA_NODE_CONTAINER.close();
  }
}
