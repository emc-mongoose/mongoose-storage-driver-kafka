package com.emc.mongoose.storage.driver.kafka.integration;

import com.emc.mongoose.storage.driver.kafka.KafkaNode;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.*;

import java.util.*;

import static org.junit.Assert.assertTrue;

public class KafkaTopicTest {

    private final List<String> topicNames = Arrays.asList("test-topic-1","test-topic-2","test-topic-3");

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
        adminClient.createTopics(createListNewTopicsFromNames(Collections.singletonList("test-topic")))
                .all().get();
        Set<String> topics = adminClient.listTopics().names().get();
        assertTrue("Topic \"test-topic\" is not created", topics.contains("test-topic"));
    }


    //to run this test should be enabled "delete.topic.enable=true" in kafka broker configuration
    /*@Test
    public void deleteTopicTest() throws Exception{
        adminClient.createTopics(createListNewTopicsFromNames(Collections.singletonList("test-topic"))).all().get();
        Map<String, KafkaFuture<Void>> deletedTopics = adminClient.deleteTopics(Collections.singleton("test-topic")).values();
        assertEquals(deletedTopics.size(),1);
        assertTrue(deletedTopics.containsKey("test-topic"));
        deletedTopics.get("test-topic").get(1000, TimeUnit.MICROSECONDS);
    }*/

    @Test
    public void listTopicsTest() throws Exception{
        adminClient.createTopics(createListNewTopicsFromNames(topicNames));
        Set<String> topics = adminClient.listTopics().names().get();
        for (String topicName : topicNames) {
            assertTrue(topics.contains(topicName));
        }
    }

    private List<NewTopic> createListNewTopicsFromNames(List<String> names){
        List<NewTopic> topics = new ArrayList<>();
        for (String name : names) {
            topics.add(new NewTopic(name,1,(short) 1));
        }
        return topics;
    }

}
