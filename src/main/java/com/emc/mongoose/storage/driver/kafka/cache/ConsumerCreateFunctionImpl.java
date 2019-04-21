package com.emc.mongoose.storage.driver.kafka.cache;

import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ConsumerCreateFunctionImpl implements ConsumerCreateFunction {

  private Properties properties;

  @Override
  public KafkaConsumer apply(String name) {
    properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, name);
    return new KafkaConsumer(properties);
  }
}
