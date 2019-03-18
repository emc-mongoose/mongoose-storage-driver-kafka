package com.emc.mongoose.storage.driver.kafka.util.docker;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.command.CreateNetworkResponse;
import com.github.dockerjava.api.exception.NotFoundException;
import com.github.dockerjava.core.DockerClientBuilder;
import com.github.dockerjava.core.command.PullImageResultCallback;
import java.io.Closeable;
import java.util.logging.Logger;

public class ZookeeperNodeContainer implements Closeable {

  private static final Logger LOG = Logger.getLogger(ZookeeperNodeContainer.class.getSimpleName());
  private static final String IMAGE_NAME = "zookeeper:3.4";
  private static final DockerClient DOCKER_CLIENT = DockerClientBuilder.getInstance().build();
  private static String NETWORK_ID = null;
  private static String ZOOKEEPER_CONTAINER_ID = null;

  public ZookeeperNodeContainer() throws Exception {
    try {
      DOCKER_CLIENT.inspectImageCmd(IMAGE_NAME).exec();
    } catch (final NotFoundException e) {
      DOCKER_CLIENT.pullImageCmd(IMAGE_NAME).exec(new PullImageResultCallback()).awaitCompletion();
    }

    final CreateNetworkResponse network =
        DOCKER_CLIENT.createNetworkCmd().withName("kafka-net").exec();
    NETWORK_ID = network.getId();
    LOG.info("docker network create " + NETWORK_ID + "...");

    final CreateContainerResponse container =
        DOCKER_CLIENT
            .createContainerCmd(IMAGE_NAME)
            .withName("zookeeper")
            .withNetworkMode("host") // --network kafka-net
            .withAttachStderr(true)
            .withAttachStdout(true)
            .exec();
    ZOOKEEPER_CONTAINER_ID = container.getId();
    LOG.info("docker start " + ZOOKEEPER_CONTAINER_ID + "...");
    DOCKER_CLIENT.startContainerCmd(ZOOKEEPER_CONTAINER_ID).exec();
  }

  public static String getContainerId() {
    return ZOOKEEPER_CONTAINER_ID;
  }

  public final void close() {
    if (ZOOKEEPER_CONTAINER_ID != null) {
      LOG.info("docker kill " + ZOOKEEPER_CONTAINER_ID + "...");
      DOCKER_CLIENT.killContainerCmd(ZOOKEEPER_CONTAINER_ID).exec();
      LOG.info("docker rm " + ZOOKEEPER_CONTAINER_ID + "...");
      DOCKER_CLIENT.removeContainerCmd(ZOOKEEPER_CONTAINER_ID).exec();
      ZOOKEEPER_CONTAINER_ID = null;
    }
    if (NETWORK_ID != null) {
      LOG.info("docker network rm " + NETWORK_ID + "...");
      DOCKER_CLIENT.removeNetworkCmd(NETWORK_ID).exec();
    }
  }
}
