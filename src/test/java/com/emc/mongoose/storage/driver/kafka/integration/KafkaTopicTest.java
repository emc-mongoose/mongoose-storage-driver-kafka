package com.emc.mongoose.storage.driver.kafka.integration;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.emc.mongoose.storage.driver.kafka.util.docker.KafkaNodeContainer;
import java.net.Socket;
import java.util.Collections;
import org.apache.kafka.clients.admin.*;
import org.junit.*;

public class KafkaTopicTest {
  private static KafkaNodeContainer kafkaNodeContainer;

  private AdminClient adminClient;

  @BeforeClass
  public static void initClass() throws Exception {
    kafkaNodeContainer = new KafkaNodeContainer();
  }

  @AfterClass
  public static void shutDownClass() {
    kafkaNodeContainer.close();
  }

  @Before
  public void initTest() {
    final String ip = kafkaNodeContainer.getKafkaIp();
    adminClient =
        AdminClient.create(
            Collections.singletonMap(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, ip));
  }

  @After
  public void shutDownTest() {
    adminClient.close();
  }

  @Test
  public final void testConnectivity() throws Exception {
    Thread.sleep(150000);
    try (final var socket = new Socket("127.0.0.1", 2181)) {
      assertTrue(
          "Not connected to " + kafkaNodeContainer.getZookeeperContainerIp() + ":" + 2181,
          socket.isConnected());
      assertFalse(
          "Closed by server: " + kafkaNodeContainer.getZookeeperContainerIp() + ":" + 2181,
          socket.isClosed());
    }
    try (final var socket = new Socket("127.0.0.1", 9092)) {
      assertTrue(
          "Not connected to " + kafkaNodeContainer.getContainerIp() + ":" + 9092,
          socket.isConnected());
      assertFalse(
          "Closed by server: " + kafkaNodeContainer.getContainerIp() + ":" + 9092,
          socket.isClosed());
    }
  }

  //  @Test
  //  public void createSingleTopicTest() throws Exception {
  //    adminClient
  //        .createTopics(Collections.singleton(new NewTopic("test-topic", 1, (short) 1)))
  //        .all()
  //        .get();
  //    final Set<String> topics = adminClient.listTopics().names().get();
  //    assertTrue("Topic \"test-topic\" is not created", topics.contains("test-topic"));
  //  }

  //  @Test
  //  public void createTopic() throws Exception {
  //    final CreateTopicsResult result =
  //            adminClient.createTopics(Collections.singleton(new NewTopic("test-topic", 2, (short)
  // 1)));
  //    Assert.assertTrue(
  //            "Topic with specified name \"test-topic\" wasn't created\n",
  //            result.values().containsKey("test-topic"));
  //  }

  //  @Test
  //  public void listTopic1sTest() throws Exception {
  //    adminClient.createTopics(
  //        Arrays.asList(
  //            new NewTopic("test-topic-1", 1, (short) 1),
  //            new NewTopic("test-topic-2", 1, (short) 1),
  //            new NewTopic("test-topic-3", 1, (short) 1)));
  //    final Set<String> topics = adminClient.listTopics().names().get();
  //    assertTrue("topic \"test-topic-1\" is not created", topics.contains("test-topic-1"));
  //    assertTrue("topic \"test-topic-2\" is not created", topics.contains("test-topic-2"));
  //    assertTrue("topic \"test-topic-3\" is not created", topics.contains("test-topic-3"));
  //  }

  //  @Test
  //  public void listTopic1sTest() throws Exception {
  //    final CreateTopicsResult result =
  //        adminClient.createTopics(
  //            Arrays.asList(
  //                new NewTopic("test-topic-1", 1, (short) 1),
  //                new NewTopic("test-topic-2", 1, (short) 1),
  //                new NewTopic("test-topic-3", 1, (short) 1)));
  //
  //    assertTrue(
  //        "topic \"test-topic-1\" is not created", result.values().containsKey("test-topic-1"));
  //    assertTrue(
  //        "topic \"test-topic-2\" is not created", result.values().containsKey("test-topic-2"));
  //    assertTrue(
  //        "topic \"test-topic-3\" is not created", result.values().containsKey("test-topic-3"));
  //
  //    final ListTopicsResult topicsResult = adminClient.listTopics();
  //    System.out.println(topicsResult.namesToListings().get().toString());
  //    final KafkaFuture<Set<String>> kafkaFuture = topicsResult.names();
  //    System.out.println(kafkaFuture.get(180, TimeUnit.SECONDS));
  //  }
}
