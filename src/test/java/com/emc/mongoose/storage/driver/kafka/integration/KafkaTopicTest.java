package com.emc.mongoose.storage.driver.kafka.integration;

import static org.junit.Assert.assertTrue;

import com.emc.mongoose.storage.driver.kafka.KafkaNode;
import java.util.*;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.*;

public class KafkaTopicTest {

  private AdminClient adminClient;

  private static Properties props;

  @BeforeClass
  public static void initClass() throws Exception {
    props = new Properties();
    final var addrWithPort = KafkaNode.addr() + ":" + KafkaNode.PORT;
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, addrWithPort);
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
    final var opts = new CreateTopicsOptions().timeoutMs(60_000);
    adminClient.createTopics(topicsToCreate, opts).all().get();
    Set<String> topics = adminClient.listTopics().names().get();
    assertTrue("Topic \"test-topic-0\" is not created", topics.contains("test-topic-0"));
  }

  @Test
  public void listTopicsTest() throws Exception {
    adminClient.createTopics(
        Arrays.asList(
            new NewTopic("test-topic-1", 1, (short) 1),
            new NewTopic("test-topic-2", 1, (short) 1),
            new NewTopic("test-topic-3", 1, (short) 1)));
    Set<String> topics = adminClient.listTopics().names().get();
    assertTrue("topic \"test-topic-1\" is not created", topics.contains("test-topic-1"));
    assertTrue("topic \"test-topic-2\" is not created", topics.contains("test-topic-2"));
    assertTrue("topic \"test-topic-3\" is not created", topics.contains("test-topic-3"));
  }
}
