package com.emc.mongoose.storage.driver.kafka.integration;

import static com.emc.mongoose.base.Constants.APP_NAME;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.emc.mongoose.base.data.DataInput;
import com.emc.mongoose.base.env.Extension;
import com.emc.mongoose.base.item.DataItem;
import com.emc.mongoose.base.storage.driver.StorageDriver;
import com.emc.mongoose.storage.driver.kafka.KafkaStorageDriver;
import com.emc.mongoose.storage.driver.kafka.util.docker.KafkaNodeContainer;
import com.github.akurilov.commons.collection.TreeUtil;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.SchemaProvider;
import com.github.akurilov.confuse.impl.BasicConfig;
import java.util.*;
import java.util.stream.Collectors;
import lombok.val;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.*;

public class ReadRecordDriverTest {
  private static KafkaNodeContainer kafkaNodeContainer;

  private AdminClient adminClient;
  private Producer<String, String> producer;
  private static Properties props;

  private DataInput dataInput;
  private StorageDriver driver;
  private List<DataItem> evtItems;

  private static final String TOPIC_NAME = "topic" + ReadRecordDriverTest.class.getSimpleName();
  private static final String KEY_NAME = "key" + ReadRecordDriverTest.class.getSimpleName();
  private static final String DATA = "test-record";

  @BeforeClass
  public static void initClass() throws Exception {
    kafkaNodeContainer = new KafkaNodeContainer();
  }

  @AfterClass
  public static void shutDownClass() {
    kafkaNodeContainer.close();
  }

  @Before
  public void initTest() {
    final String ip = kafkaNodeContainer.getKafkaIp();
    adminClient =
        AdminClient.create(
            Collections.singletonMap(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, ip));
    props = new Properties();
    props.put(
        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaNodeContainer.getContainerIp() + ":9092");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("group.id", "null");
    producer = new KafkaProducer<>(props);
  }

  @After
  public void shutDownTest() {
    adminClient.close();
  }

  @Test
  public void readRecordsTest() throws Exception {
    final CreateTopicsResult result =
        adminClient.createTopics(Collections.singleton(new NewTopic(TOPIC_NAME, 2, (short) 1)));
    Assert.assertTrue(
        "Topic with specified name \"" + TOPIC_NAME + "\" wasn't created\n",
        result.values().containsKey(TOPIC_NAME));
    final Set<String> topics = adminClient.listTopics().names().get();
    assertTrue(
        "Topic \"" + TOPIC_NAME + "\" is not in the topics list", topics.contains(TOPIC_NAME));

    final ProducerRecord<String, String> producerRecord =
        new ProducerRecord<>(TOPIC_NAME, KEY_NAME, DATA);
    producer.send(producerRecord);

    dataInput = DataInput.instance(null, "7a42d9c483244167", new SizeInBytes(1024 * 1024 - 8), 16);
    val config = getConfig();
    driver =
        new KafkaStorageDriver(
            getClass().getSimpleName(), dataInput, config.configVal("storage"), false, 32768);
    driver.start();
  }

  static Config getConfig() {
    try {
      val configSchemas =
          Extension.load(Thread.currentThread().getContextClassLoader()).stream()
              .map(Extension::schemaProvider)
              .filter(Objects::nonNull)
              .map(
                  schemaProvider -> {
                    try {
                      return schemaProvider.schema();
                    } catch (final Exception e) {
                      fail(e.getMessage());
                    }
                    return null;
                  })
              .filter(Objects::nonNull)
              .collect(Collectors.toList());
      SchemaProvider.resolve(APP_NAME, Thread.currentThread().getContextClassLoader()).stream()
          .findFirst()
          .ifPresent(configSchemas::add);
      val configSchema = TreeUtil.reduceForest(configSchemas);
      val config = new BasicConfig("-", configSchema);

      config.val("load-op-timeoutMillis", 300000);

      config.val("storage-net-rcvBuf", 0);
      config.val("storage-net-sndBuf", 0);
      config.val("storage-net-linger", 0);
      config.val("storage-net-node-addrs", "127.0.0.1");
      config.val("storage-net-node-port", 9092);

      config.val("storage-driver-create-key-enabled", false);
      config.val("storage-driver-read-timeoutMillis", 10000);
      config.val("storage-driver-request-size", 1048576);
      config.val("storage-driver-buffer-memory", 33554432);
      config.val("storage-driver-compression-type", "none");

      return config;
    } catch (final Throwable cause) {
      throw new RuntimeException(cause);
    }
  }
}
