package com.emc.mongoose.storage.driver.kafka;

import com.emc.mongoose.base.config.IllegalConfigurationException;
import com.emc.mongoose.base.data.DataInput;
import com.emc.mongoose.base.item.Item;
import com.emc.mongoose.base.item.ItemFactory;
import com.emc.mongoose.base.item.op.OpType;
import com.emc.mongoose.base.item.op.Operation;
import com.emc.mongoose.base.item.op.data.DataOperation;
import com.emc.mongoose.base.item.op.path.PathOperation;
import com.emc.mongoose.base.storage.Credential;
import com.emc.mongoose.storage.driver.coop.CoopStorageDriverBase;
import com.emc.mongoose.storage.driver.kafka.cache.AdminClientCreateFunctionImpl;
import com.emc.mongoose.storage.driver.kafka.cache.ProducerCreateFunctionImpl;
import com.github.akurilov.confuse.Config;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.*;

import static com.emc.mongoose.base.item.op.Operation.Status.FAIL_UNKNOWN;
import java.io.IOException;
import java.util.Collections;
import java.util. List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.emc.mongoose.base.item.op.Operation.Status.SUCC;
import static com.github.akurilov.commons.lang.Exceptions.throwUnchecked;

public class KafkaStorageDriver<I extends Item, O extends Operation<I>> extends CoopStorageDriverBase<I, O> {

  private final String[] endpointAddrs;
  private final int nodePort;
  private boolean key;
  private int numPartition;
  private short replicationFactor;
  private int request;
  private int batch;
  private int sndBuf;
  private int rcvBuf;
  private int linger;
  private long buffer;
  private String compression;
  private final AtomicInteger rrc = new AtomicInteger(0);
  private final Map<Properties , AdminClientCreateFunctionImpl> adminClientCreateFuncCache = new ConcurrentHashMap<>();
  private final Map<String, AdminClient> adminClientCache = new ConcurrentHashMap<>();
  private final Map<Properties, ProducerCreateFunctionImpl> producerCreateFuncCache = new ConcurrentHashMap<>();
  private final Map<String, KafkaProducer> producerCache = new ConcurrentHashMap<>();
  private final Map<String, NewTopic> topicCache = new ConcurrentHashMap<>();

  public KafkaStorageDriver(String testStepId, DataInput dataInput, Config storageConfig, boolean verifyFlag, int batchSize) throws IllegalConfigurationException {
    super(testStepId, dataInput, storageConfig, verifyFlag, opTimeOut);
    var driverConfig = storageConfig.configVal("driver");
    this.key = driverConfig.boolVal("create-key-enabled");
    this.request = driverConfig.intVal(s"request-size");
    this.batch = driverConfig.intVal("batch-size");
    var netConfig = storageConfig.configVal("net");
    this.buffer = driverConfig.longVal("buffer-memory");
    this.compression = driverConfig.stringVal("compression-type");
    var nodeConfig = netConfig.configVal("node");
    this.nodePort = nodeConfig.intVal("port");
    var endpointAddrList = nodeConfig.listVal("addrs");
    this.endpointAddrs = endpointAddrList.toArray(new String[endpointAddrList.size()]);
    this.sndBuf = netConfig.intVal("sndBuf");
    this.rcvBuf = netConfig.intVal("rcvBuf");
    this.linger = netConfig.intVal("linger");
    this.numPartition = 1;
    this.replicationFactor = 1;
  }

  @Override
  protected final boolean submit(final O op) throws IllegalStateException {
    if (concurrencyThrottle.tryAcquire()) {
      final var opType = op.type();
      var nodeAddr = op.nodeAddr();
      if (op instanceof DataOperation) {
        submitRecordOperation((DataOperation) op, opType, nodeAddr);
      } else if (op instanceof PathOperation) {
        submitTopicOperation((PathOperation) op, opType);
      } else {
        throw new AssertionError(
          "storage driver doesn't support the token operations");
      }
    }
    return true;
  }

  private void submitRecordOperation(DataOperation op, OpType opType, String nodeAddr) {
      switch (opType) {
        case CREATE:
          submitRecordCreateOperation(op, nodeAddr);
          break;
        case READ:
          submitRecordReadOperation();
          break;
        case UPDATE:
          throw new AssertionError("Not implemented");
        case DELETE:
          submitRecordDeleteOperation();
          break;
        case LIST:
          throw new AssertionError("Not implemented");
        default:
          throw new AssertionError("Not implemented");
      }
  }

  private void submitRecordDeleteOperation() {

  }

  private void submitRecordReadOperation() {

  }

  boolean completeFailedOperation(final O op, final Throwable thrown) {
    thrown.printStackTrace();
    return completeOperation(op, FAIL_UNKNOWN);
  }

  boolean completeOperation(final O op, final Operation.Status status) {
    concurrencyThrottle.release();
    op.status(status);
    op.finishRequest();
    op.startResponse();
    op.finishResponse();
    return handleCompleted(op);
  }

  private Properties createAdminConfig(String nodeAddr){
    var adminConfig = new Properties();
    adminConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, nodeAddr);
    adminConfig.put(AdminClientConfig.SEND_BUFFER_CONFIG, this.sndBuf);
    adminConfig.put(AdminClientConfig.RECEIVE_BUFFER_CONFIG, this.rcvBuf);
    return adminConfig;
  }
  private Properties createProducerConfig(String nodeAddr){
    var producerConfig = new Properties();
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,nodeAddr);
    producerConfig.put(ProducerConfig.BATCH_SIZE_CONFIG, this.batch);
    producerConfig.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, this.request);
    producerConfig.put(ProducerConfig.BUFFER_MEMORY_CONFIG, this.buffer);
    producerConfig.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compression);
    producerConfig.put(ProducerConfig.SEND_BUFFER_CONFIG, this.sndBuf);
    producerConfig.put(ProducerConfig.LINGER_MS_CONFIG, this.linger);
    producerConfig.put(ProducerConfig.RECEIVE_BUFFER_CONFIG, this.rcvBuf);
    producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"com.emc.mongoose.storage.driver.kafka.io.DataItemSerializer");
    return producerConfig;
  }

  private void submitRecordCreateOperation(final DataOperation recordOp, String nodeAddr) {
    try {
      var adminConfig = adminClientCreateFuncCache.computeIfAbsent(createAdminConfig(nodeAddr), AdminClientCreateFunctionImpl::new);
      var adminClient = adminClientCache.computeIfAbsent(nodeAddr, adminConfig);
      var producerConfig = producerCreateFuncCache.computeIfAbsent(createProducerConfig(nodeAddr), ProducerCreateFunctionImpl::new);
      var kafkaProducer = producerCache.computeIfAbsent(nodeAddr,producerConfig);
      var recordItem = recordOp.item();
      var topicName = recordItem.name();
      if (topicCache.get(topicName)==null) {
        final NewTopic topic =  new NewTopic(topicName, numPartition, replicationFactor);
        topicCache.put(topicName, topic);
        adminClient.createTopics(Collections.singletonList(topic));
      }
      Callback callback = (RecordMetadata metadata, Exception exception) -> {
        if (exception != null) {
          completeFailedOperation((O) recordOp, exception);
        } else {
           completeOperation((O) recordItem, SUCC);
        }
      };
      if (key) {
        var producerKey = Long.toString(recordItem.offset(), Character.MAX_RADIX);
        kafkaProducer.send(new ProducerRecord<>(topicName, producerKey, recordItem), callback);
      } else {
        kafkaProducer.send(new ProducerRecord<>(topicName, recordItem), callback);
      }
      recordOp.startRequest();
    }catch( final NullPointerException e){
        completeFailedOperation((O) recordOp, e);
    }
    catch( final Throwable thrown)
    {
      if (thrown instanceof InterruptedException) {
        throwUnchecked(thrown);
      }
      completeFailedOperation((O) recordOp, thrown);
    }
  }


  private void submitTopicOperation(PathOperation op, OpType opType) {
    switch (opType) {
      case CREATE:
        submitTopicCreateOperation();
        break;
      case READ:
        submitTopicReadOperation();
        break;
      case UPDATE:
        throw new AssertionError("Not implemented");
      case DELETE:
        submitTopicDeleteOperation();
        break;
      case LIST:
        submitTopicDeleteOperation();
      default:
        throw new AssertionError("Not implemented");
    }
  }

  private void submitTopicCreateOperation() {

  }

  private void submitTopicReadOperation() {

  }

  private void submitTopicDeleteOperation() {

  }

  @Override
  protected final int submit(final List<O> ops, final int from, final int to)
    throws IllegalStateException {
    for (var i = from; i < to; i++) {
      if (!submit(ops.get(i))) {
        return i - from;
      }
    }
    return to - from;
  }

  @Override
  protected final int submit(final List<O> ops)
    throws IllegalStateException {
    final var opsCount = ops.size();
    for (var i = 0; i < opsCount; i++) {
      if (!submit(ops.get(i))) {
        return i;
      }
    }
    return opsCount;
  }

  String nextEndpointAddr() {
    return endpointAddrs[rrc.getAndIncrement() % endpointAddrs.length];
  }

  @Override
  protected boolean prepare(final O operation) {
    super.prepare(operation);
    var endpointAddr = operation.nodeAddr();
    if (endpointAddr == null) {
      endpointAddr = nextEndpointAddr()+":"+this.nodePort;
      operation.nodeAddr(endpointAddr);
    }
    return true;
  }

  @Override
  protected String requestNewPath(String path) {
    throw new AssertionError("Should not be invoked");  }

  @Override
  protected String requestNewAuthToken(Credential credential) {

    throw new AssertionError("Should not be invoked");
  }

  @Override
  public List<I> list(ItemFactory<I> itemFactory, String path, String prefix, int idRadix, I lastPrevItem, int count) throws IOException {
    return null;
  }

  @Override
  public void adjustIoBuffers(long avgTransferSize, OpType opType) {

  }
}

