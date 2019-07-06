package com.emc.mongoose.storage.driver.kafka.cache;

import java.util.function.Function;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public interface ConsumerCreateFunction extends Function<String, KafkaConsumer> {

  @Override
  KafkaConsumer apply(final String name);
}
