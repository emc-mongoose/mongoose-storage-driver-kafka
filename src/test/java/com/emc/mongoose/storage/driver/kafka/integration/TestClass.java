package com.emc.mongoose.storage.driver.kafka.integration;

import com.emc.mongoose.storage.driver.kafka.util.docker.KafkaNodeContainer;
import com.emc.mongoose.storage.driver.kafka.util.docker.ZookeeperNodeContainer;
import org.junit.Test;

public class TestClass {
    private static ZookeeperNodeContainer ZOOKEEPER_NODE_CONTAINER;
    private static KafkaNodeContainer KAFKA_NODE_CONTAINER;
    public TestClass() throws Exception { }

    @Test
    public void createContainers()
            throws Exception {
        try {
            ZOOKEEPER_NODE_CONTAINER = new ZookeeperNodeContainer();
            KAFKA_NODE_CONTAINER = new KafkaNodeContainer();
        } catch(final Exception e) {
            throw new AssertionError(e);
        }
    }
}
