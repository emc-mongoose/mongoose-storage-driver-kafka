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
    final var topic = getClass().getSimpleName();
    final var topicsToCreate = Collections.singleton(new NewTopic(topic, 2, (short) 1));
    final var options = new CreateTopicsOptions().timeoutMs(60_000);
    CreateTopicsResult result = adminClient.createTopics(topicsToCreate, options);
    Assert.assertTrue(
        "Topic with specified name \"" + topic + "\" wasn't created\n",
        result.values().containsKey(topic));
  }

  @BeforeClass
  public static void setupClass() throws Exception {
    properties.put(
        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaNode.addr() + ":" + KafkaNode.PORT);
    adminClient = KafkaAdminClient.create(properties);
  }
}
