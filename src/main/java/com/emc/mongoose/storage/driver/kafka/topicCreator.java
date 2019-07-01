package com.emc.mongoose.storage.driver.kafka;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import lombok.val;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;

public class topicCreator {
  private static final String TOPIC_NAME = "topic";

  public static void main(String[] args) {

    final Properties admProps = new Properties();
    admProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    final AdminClient adm = KafkaAdminClient.create(admProps);

    try {
      final NewTopic newTopic = new NewTopic(TOPIC_NAME, 1, (short) 1);
      val createTopicsResult = adm.createTopics(Collections.singleton(newTopic));
      createTopicsResult.values().get(TOPIC_NAME).get();
    } catch (InterruptedException | ExecutionException e) {
      if (!(e.getCause() instanceof TopicExistsException)) {
        throw new RuntimeException(e.getMessage(), e);
      } else {
        System.out.println("Topic already exists");
      }
    }
  }
}
