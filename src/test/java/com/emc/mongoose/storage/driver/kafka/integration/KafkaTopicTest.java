package com.emc.mongoose.storage.driver.kafka.integration;

import static org.junit.Assert.assertTrue;

import com.emc.mongoose.storage.driver.kafka.util.docker.KafkaNodeContainer;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.*;

public class KafkaTopicTest {

  private AdminClient adminClient;
  private static KafkaNodeContainer KAFKA_NODE_CONTAINER;

  private static Properties props;

  @BeforeClass
  public static void initClass() throws Exception {
    KAFKA_NODE_CONTAINER = new KafkaNodeContainer();
    props = new Properties();
    final var addrWithPort = KAFKA_NODE_CONTAINER.getContainerIp() + ":9092";
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, addrWithPort);
  }

  @AfterClass
  public static void tearDownClass() {
    KAFKA_NODE_CONTAINER.close();
  }

  @Before
  public void initTest() {
    adminClient = AdminClient.create(props);
  }

  @After
  public void shutDownTest() {
    adminClient.close();
  }

  @Test
  public void createSingleTopicTest() throws Exception {
    final var topicsToCreate = Collections.singleton(new NewTopic("test-topic-0", 1, (short) 1));
    try {
      adminClient.createTopics(topicsToCreate).all().get(1, TimeUnit.MINUTES);
    } catch (final ExecutionException | TimeoutException ignore) {
    }
    Set<String> topics = adminClient.listTopics().names().get();
    assertTrue("Topic \"test-topic-0\" is not created", topics.contains("test-topic-0"));
  }

  @Test
  public void listTopicsTest() throws Exception {
    try {
      adminClient
          .createTopics(
              Arrays.asList(
                  new NewTopic("test-topic-1", 1, (short) 1),
                  new NewTopic("test-topic-2", 1, (short) 1),
                  new NewTopic("test-topic-3", 1, (short) 1)))
          .all()
          .get(1, TimeUnit.MINUTES);
    } catch (final ExecutionException | TimeoutException ignore) {
    }
    Set<String> topics = adminClient.listTopics().names().get();
    assertTrue("topic \"test-topic-1\" is not created", topics.contains("test-topic-1"));
    assertTrue("topic \"test-topic-2\" is not created", topics.contains("test-topic-2"));
    assertTrue("topic \"test-topic-3\" is not created", topics.contains("test-topic-3"));
  }
}
