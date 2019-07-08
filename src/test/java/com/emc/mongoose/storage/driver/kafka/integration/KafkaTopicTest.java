package com.emc.mongoose.storage.driver.kafka.integration;

import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import org.apache.kafka.clients.admin.*;
import org.junit.*;

public class KafkaTopicTest {
  private static final String IP = "localhost:9092";
  private static final String TOPIC_NAME =
      "topic" + KafkaTopicTest.class.getSimpleName() + System.currentTimeMillis();
  private static final String TOPIC_NAME1 =
      "topic1" + KafkaTopicTest.class.getSimpleName() + System.currentTimeMillis();
  private static final String TOPIC_NAME2 =
      "topic2" + KafkaTopicTest.class.getSimpleName() + System.currentTimeMillis();
  private static final String TOPIC_NAME3 =
      "topic3" + KafkaTopicTest.class.getSimpleName() + System.currentTimeMillis();
  private AdminClient adminClient;

  @Before
  public void initTest() {
    adminClient =
        AdminClient.create(
            Collections.singletonMap(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, IP));
  }

  @Test
  public void createSingleTopicTest() throws Exception {
    final CreateTopicsResult result =
        adminClient.createTopics(Collections.singleton(new NewTopic(TOPIC_NAME, 2, (short) 1)));
    Assert.assertTrue(
        "Topic with specified name \"" + TOPIC_NAME + "\" wasn't created\n",
        result.values().containsKey(TOPIC_NAME));
    final Set<String> topics = adminClient.listTopics().names().get();
    assertTrue(
        "Topic \"" + TOPIC_NAME + "\" is not in the topics list", topics.contains(TOPIC_NAME));
  }

  @Test
  public void listTopicsTest() throws Exception {
    final CreateTopicsResult result =
        adminClient.createTopics(
            Arrays.asList(
                new NewTopic(TOPIC_NAME1, 1, (short) 1),
                new NewTopic(TOPIC_NAME2, 1, (short) 1),
                new NewTopic(TOPIC_NAME3, 1, (short) 1)));

    assertTrue(
        "topic \"" + TOPIC_NAME1 + "\" wasn't created", result.values().containsKey(TOPIC_NAME1));
    assertTrue(
        "topic \"" + TOPIC_NAME2 + "\" wasn't created", result.values().containsKey(TOPIC_NAME2));
    assertTrue(
        "topic \"" + TOPIC_NAME3 + "\" wasn't created", result.values().containsKey(TOPIC_NAME3));
    final Set<String> topics = adminClient.listTopics().names().get();
    assertTrue(
        "topic \"" + TOPIC_NAME1 + "\" is not in the topics list", topics.contains(TOPIC_NAME1));
    assertTrue(
        "topic \"" + TOPIC_NAME2 + "\" is not in the topics list", topics.contains(TOPIC_NAME2));
    assertTrue(
        "topic \"" + TOPIC_NAME3 + "\" is not in the topics list", topics.contains(TOPIC_NAME3));
  }

  @After
  public void shutDownTest() {
    adminClient.close();
  }
}
