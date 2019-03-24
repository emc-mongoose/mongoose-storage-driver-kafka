package com.emc.mongoose.storage.driver.kafka.integration;

import com.emc.mongoose.storage.driver.kafka.KafkaNode;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
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
    final var results = adminClient.createTopics(topicsToCreate).values();
    Assert.assertTrue(
        "Topic with specified name \"" + topic + "\" wasn't created\n", results.containsKey(topic));
    results.get(topic).get(60, TimeUnit.SECONDS);
  }

  @BeforeClass
  public static void setupClass() throws Exception {
    properties.put(
        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaNode.addr() + ":" + KafkaNode.PORT);
    adminClient = KafkaAdminClient.create(properties);
  }
}
