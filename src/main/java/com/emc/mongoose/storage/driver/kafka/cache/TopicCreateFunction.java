package com.emc.mongoose.storage.driver.kafka.cache;

import java.util.function.Function;
import org.apache.kafka.clients.admin.NewTopic;

public interface TopicCreateFunction extends Function<String, NewTopic> {

  @Override
  NewTopic apply(final String name);
}
