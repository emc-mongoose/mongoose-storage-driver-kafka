package com.emc.mongoose.storage.driver.kafka.cache;

import java.util.Collections;
import lombok.Value;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;

@Value
public class TopicCreateFunctionImpl implements TopicCreateFunction {

  AdminClient adminClient;

  @Override
  public NewTopic apply(String name) {
    NewTopic newTopic = new NewTopic(name, 1, (short) 1);
    adminClient.createTopics(Collections.singletonList(newTopic));
    return newTopic;
  }
}
