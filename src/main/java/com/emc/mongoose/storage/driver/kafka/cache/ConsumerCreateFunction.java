package com.emc.mongoose.storage.driver.kafka.cache;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.function.Function;

public interface ConsumerCreateFunction extends Function<String, KafkaConsumer>
{

    @Override
    KafkaConsumer apply(final String name);
}
