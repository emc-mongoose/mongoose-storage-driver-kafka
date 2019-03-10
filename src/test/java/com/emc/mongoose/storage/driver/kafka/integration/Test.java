package com.emc.mongoose.storage.driver.kafka.integration;

import com.emc.mongoose.storage.driver.kafka.util.docker.KafkaNodeContainer;
import org.junit.BeforeClass;

public class Test {
    private static KafkaNodeContainer KAFKA_NODE_CONTAINER;
    public Test () throws Exception { }

    @BeforeClass
    public static void setUpClass()
            throws Exception {
        try {
            KAFKA_NODE_CONTAINER = new KafkaNodeContainer();
        } catch(final Exception e) {
            throw new AssertionError(e);
        }
    }
}
