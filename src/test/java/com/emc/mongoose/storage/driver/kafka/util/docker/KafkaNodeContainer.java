package com.emc.mongoose.storage.driver.kafka.util.docker;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.exception.NotFoundException;
import com.github.dockerjava.core.DockerClientBuilder;
import com.github.dockerjava.core.command.PullImageResultCallback;
import java.io.Closeable;
import java.util.logging.Logger;

public class KafkaNodeContainer implements Closeable {

  private static final Logger LOG = Logger.getLogger(KafkaNodeContainer.class.getSimpleName());
  private static final String IMAGE_NAME = "ches/kafka:latest";
  private static final DockerClient DOCKER_CLIENT = DockerClientBuilder.getInstance().build();
  private static ZookeeperNodeContainer ZOOKEEPER_NODE_CONTAINER;

  private static String KAFKA_CONTAINER_ID = null;

  public KafkaNodeContainer() throws Exception {
    try {
      DOCKER_CLIENT.inspectImageCmd(IMAGE_NAME).exec();
    } catch (final NotFoundException e) {
      DOCKER_CLIENT.pullImageCmd(IMAGE_NAME).exec(new PullImageResultCallback()).awaitCompletion();
    }

    ZOOKEEPER_NODE_CONTAINER = new ZookeeperNodeContainer();

    final CreateContainerResponse container =
        DOCKER_CLIENT
            .createContainerCmd(IMAGE_NAME)
            .withName("kafka")
            .withNetworkMode("kafka-net") // --network kafka-net
            .withEnv("ZOOKEEPER_IP=zookeeper")
            .withAttachStderr(true)
            .withAttachStdout(true)
            .exec();
    KAFKA_CONTAINER_ID = container.getId();
    LOG.info("docker start " + KAFKA_CONTAINER_ID + "...");
    DOCKER_CLIENT.startContainerCmd(KAFKA_CONTAINER_ID).exec();
  }

  public final void close() {
    if (KAFKA_CONTAINER_ID != null) {
      LOG.info("docker kill " + KAFKA_CONTAINER_ID + "...");
      DOCKER_CLIENT.killContainerCmd(KAFKA_CONTAINER_ID).exec();
      LOG.info("docker rm " + KAFKA_CONTAINER_ID + "...");
      DOCKER_CLIENT.removeContainerCmd(KAFKA_CONTAINER_ID).exec();
      KAFKA_CONTAINER_ID = null;
    }
    ZOOKEEPER_NODE_CONTAINER.close();
  }
}
