package com.emc.mongoose.storage.driver.kafka.integration;

import com.emc.mongoose.storage.driver.kafka.KafkaNode;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.*;

import java.util.*;

import static org.junit.Assert.assertTrue;

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
    public void initTest(){
        adminClient = AdminClient.create(props);
    }

    @After
    public void shutDownTest(){
        adminClient.close();
    }

  @Test
  public void createSingleTopicTest() throws Exception {
    adminClient
        .createTopics(Collections.singleton(new NewTopic("test-topic", 1, (short) 1)))
        .all()
        .get();
    Set<String> topics = adminClient.listTopics().names().get();
    assertTrue("Topic \"test-topic\" is not created", topics.contains("test-topic"));
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
