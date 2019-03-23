package com.emc.mongoose.storage.driver.kafka.integration;

import com.emc.mongoose.storage.driver.kafka.KafkaNode;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.admin.*;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class CreateSingleTopicTest {

  private static AdminClient adminClient;
  private static Properties properties = new Properties();

  @Test
  public void createTopic() throws Exception {
    CreateTopicsResult result =
        adminClient.createTopics(Collections.singleton(new NewTopic("test-topic", 2, (short) 1)));
    Assert.assertTrue(
        "Topic with specified name \"test-topic\" wasn't created\n",
        result.values().containsKey("test-topic"));
  }

  @BeforeClass
  public static void setupClass() throws Exception {
    properties.put(
        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaNode.addr() + ":" + KafkaNode.PORT);
    adminClient = KafkaAdminClient.create(properties);
  }
}
