package com.emc.mongoose.storage.driver.kafka.cache;

import java.util.function.Function;
import org.apache.kafka.clients.producer.KafkaProducer;

public interface ProducerCreateFunction extends Function<String, KafkaProducer> {

  @Override
  KafkaProducer apply(final String name);
}
