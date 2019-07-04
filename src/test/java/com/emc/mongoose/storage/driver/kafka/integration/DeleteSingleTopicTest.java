package com.emc.mongoose.storage.driver.kafka.integration;

import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.admin.*;
import org.junit.*;

public class DeleteSingleTopicTest {

  private static AdminClient adminClient;
  private static Properties properties = new Properties();
  private static final String IP = "localhost:9092";
  private static final String topicName =
      "topic" + DeleteSingleTopicTest.class.getSimpleName() + System.currentTimeMillis();

  @Before
  public void setup() throws Exception {
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, IP);
    adminClient = KafkaAdminClient.create(properties);
    final CreateTopicsResult result =
        adminClient.createTopics(Collections.singleton(new NewTopic(topicName, 2, (short) 1)));
  }

  @Test
  public void deleteTopic() throws Exception {
    DeleteTopicsResult result = adminClient.deleteTopics(Collections.singleton(topicName));
    Assert.assertTrue(
        "Topic with specified name \"" + topicName + "\" wasn't deleted\n",
        result.values().containsKey(topicName));
  }

  @After
  public void shutDown() {
    adminClient.close();
  }
}
