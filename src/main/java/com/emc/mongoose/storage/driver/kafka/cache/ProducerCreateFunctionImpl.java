package com.emc.mongoose.storage.driver.kafka.cache;

import java.util.Properties;
import lombok.Value;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

@Value
public class ProducerCreateFunctionImpl implements ProducerCreateFunction {

  Properties properties;

  @Override
  public KafkaProducer apply(String name) {
    properties.setProperty(ProducerConfig.CLIENT_ID_CONFIG, name);
    return new KafkaProducer(properties);
  }
}
