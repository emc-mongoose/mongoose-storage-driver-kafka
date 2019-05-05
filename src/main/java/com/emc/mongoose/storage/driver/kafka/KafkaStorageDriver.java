package com.emc.mongoose.storage.driver.kafka;

import static com.emc.mongoose.base.item.op.Operation.Status.*;
import static com.github.akurilov.commons.lang.Exceptions.throwUnchecked;

import com.emc.mongoose.base.config.IllegalConfigurationException;
import com.emc.mongoose.base.data.DataInput;
import com.emc.mongoose.base.item.DataItem;
import com.emc.mongoose.base.item.Item;
import com.emc.mongoose.base.item.ItemFactory;
import com.emc.mongoose.base.item.op.OpType;
import com.emc.mongoose.base.item.op.Operation;
import com.emc.mongoose.base.item.op.data.DataOperation;
import com.emc.mongoose.base.item.op.path.PathOperation;
import com.emc.mongoose.base.logging.LogUtil;
import com.emc.mongoose.base.storage.Credential;
import com.emc.mongoose.storage.driver.coop.CoopStorageDriverBase;
import com.emc.mongoose.storage.driver.kafka.cache.AdminClientCreateFunctionImpl;
import com.emc.mongoose.storage.driver.kafka.cache.ProducerCreateFunctionImpl;
import com.emc.mongoose.storage.driver.kafka.cache.TopicCreateFunctionImpl;
import com.github.akurilov.confuse.Config;
import java.io.EOFException;
import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.val;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.logging.log4j.Level;

public class KafkaStorageDriver<I extends Item, O extends Operation<I>>
    extends CoopStorageDriverBase<I, O> {

  private final String[] endpointAddrs;
  private final int nodePort;
  private final boolean key;
  private final int requestSizeLimit;
  private final int batch;
  private final int sndBuf;
  private final int rcvBuf;
  private final int linger;
  private final long buffer;
  private final String compression;
  private final AtomicInteger rrc = new AtomicInteger(0);
  private final Map<String, Properties> configCache = new ConcurrentHashMap<>();
  private final Map<Properties, AdminClientCreateFunctionImpl> adminClientCreateFuncCache =
      new ConcurrentHashMap<>();
  private final Map<String, AdminClient> adminClientCache = new ConcurrentHashMap<>();
  private final Map<Properties, ProducerCreateFunctionImpl> producerCreateFuncCache =
      new ConcurrentHashMap<>();
  private final Map<String, KafkaProducer> producerCache = new ConcurrentHashMap<>();
  private final Map<AdminClient, TopicCreateFunctionImpl> topicCreateFuncCache =
      new ConcurrentHashMap<>();
  private final Map<String, NewTopic> topicCache = new ConcurrentHashMap<>();
  private volatile boolean listWasCalled = false;

  public KafkaStorageDriver(
      String testStepId,
      DataInput dataInput,
      Config storageConfig,
      boolean verifyFlag,
      int batchSize)
      throws IllegalConfigurationException {
    super(testStepId, dataInput, storageConfig, verifyFlag, batchSize);
    var driverConfig = storageConfig.configVal("driver");
    this.key = driverConfig.boolVal("create-key-enabled");
    this.requestSizeLimit = driverConfig.intVal("request-size");
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
    this.requestAuthTokenFunc = null;
    this.requestNewPathFunc = null;
  }

  @Override
  protected final boolean submit(final O op) throws IllegalStateException {
    if (concurrencyThrottle.tryAcquire()) {
      val opType = op.type();
      val nodeAddr = op.nodeAddr();
      if (opType.equals(OpType.NOOP)) {
        submitNoop(op);
      }

      if (op instanceof DataOperation) {
        submitRecordOperation((DataOperation) op, opType, nodeAddr);
      } else if (op instanceof PathOperation) {
        submitTopicOperation((PathOperation) op, opType);
      } else {
        throw new AssertionError("storage driver doesn't support the token operations");
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

  private void submitRecordDeleteOperation() {}

  private void submitRecordReadOperation() {}

  boolean completeFailedOperation(final O op, final Throwable thrown) {
    LogUtil.exception(Level.DEBUG, thrown, "{}: operation failed: {}", stepId, op);
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

  void completeRecordReadOperations(
      final List<O> ops, final int from, final int to, final Operation.Status status) {
    concurrencyThrottle.release();
    I item;
    O op;
    for (var i = from; i < to; i++) {
      op = ops.get(i);
      op.status(status);
      handleCompleted(op);
    }
  }

  private Properties createConfig(String nodeAddr) {
    var producerConfig = new Properties();
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, nodeAddr);
    producerConfig.put(ProducerConfig.BATCH_SIZE_CONFIG, this.batch);
    producerConfig.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, this.requestSizeLimit);
    producerConfig.put(ProducerConfig.BUFFER_MEMORY_CONFIG, this.buffer);
    producerConfig.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compression);
    producerConfig.put(ProducerConfig.SEND_BUFFER_CONFIG, this.sndBuf);
    producerConfig.put(ProducerConfig.LINGER_MS_CONFIG, this.linger);
    producerConfig.put(ProducerConfig.RECEIVE_BUFFER_CONFIG, this.rcvBuf);
    producerConfig.put(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");
    producerConfig.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "com.emc.mongoose.storage.driver.kafka.io.DataItemSerializer");
    return producerConfig;
  }

  private void submitRecordCreateOperation(final DataOperation recordOp, String nodeAddr) {
    try {
      val config = configCache.computeIfAbsent(nodeAddr, this::createConfig);
      val adminConfig =
          adminClientCreateFuncCache.computeIfAbsent(config, AdminClientCreateFunctionImpl::new);
      val adminClient = adminClientCache.computeIfAbsent(nodeAddr, adminConfig);
      val producerConfig =
          producerCreateFuncCache.computeIfAbsent(config, ProducerCreateFunctionImpl::new);
      val kafkaProducer = producerCache.computeIfAbsent(nodeAddr, producerConfig);
      val recordItem = recordOp.item();
      val topicName = recordOp.dstPath();
      val topicCreateFunc =
          topicCreateFuncCache.computeIfAbsent(adminClient, TopicCreateFunctionImpl::new);
      val topic = topicCache.computeIfAbsent(topicName, topicCreateFunc);
      if (key) {
        val producerKey = recordItem.name();
        kafkaProducer.send(
            new ProducerRecord<>(topicName, producerKey, recordItem),
            (metadata, exception) -> {
              if (exception == null) {
                try {
                  recordOp.countBytesDone(recordItem.size());
                } catch (IOException e) {
                  e.printStackTrace();
                }
                completeOperation((O) recordOp, SUCC);
              } else {
                completeFailedOperation((O) recordOp, exception);
              }
            });
      } else {
        kafkaProducer.send(
            new ProducerRecord<>(topicName, recordItem),
            (metadata, exception) -> {
              if (exception == null) {
                try {
                  recordOp.countBytesDone(recordItem.size());
                } catch (IOException e) {
                  e.printStackTrace();
                }
                completeOperation((O) recordOp, SUCC);
              } else {
                completeFailedOperation((O) recordOp, exception);
              }
            });
      }
      recordOp.startRequest();
    } catch (final NullPointerException e) {
      if (!isStarted()) {
        completeOperation((O) recordOp, INTERRUPTED);
      } else {
        completeFailedOperation((O) recordOp, e);
      }
      completeFailedOperation((O) recordOp, e);
    } catch (final Throwable thrown) {
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

  private void submitTopicCreateOperation() {}

  private void submitTopicReadOperation() {}

  private void submitTopicDeleteOperation() {}

  private int submitBatchRecordRead(final List<O> ops, final int from, final int to) {
    var submitCount = 0;
    if (from < to && concurrencyThrottle.tryAcquire()) {
      try {
        val consumerConfig = new Properties();
        val anyOp = ops.get(from);
        val nodeAddr = anyOp.nodeAddr();
        val amountOfRecords = to - from;
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, nodeAddr);
        consumerConfig.put(
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.StringDeserializer");
        consumerConfig.put(
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        consumerConfig.put(ConsumerConfig.SEND_BUFFER_CONFIG, this.sndBuf);
        consumerConfig.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, this.rcvBuf);
        consumerConfig.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(amountOfRecords));
        final KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerConfig);
        val topicNames = getTopicsFromOps(ops, from, to);
        consumer.subscribe(topicNames);
        for (var i = from; i < to; i++) {
          O recordOp = ops.get(i);
          recordOp.startRequest();
          recordOp.finishRequest();
        }
        val readRecords = consumer.poll(Duration.ZERO);
        var index = 0;
        for (ConsumerRecord<String, byte[]> record : readRecords) {
          if (record != null) {
            DataOperation recordOp = (DataOperation) ops.get(index);
            DataItem recordItem = recordOp.item();
            recordItem.size(record.value().length);
            recordOp.countBytesDone(recordItem.size());
            recordOp.startResponse();
            recordOp.finishResponse();
            index++;
          }
        }
        completeRecordReadOperations(ops, from, index, SUCC);
        submitCount = index;
        if (index < to) {
          completeRecordReadOperations(ops, index, to, RESP_FAIL_UNKNOWN);
        }
      } catch (final Throwable e) {
        LogUtil.exception(
            Level.DEBUG,
            e,
            "{}: unexpected failure while trying to read {} records",
            stepId,
            to - from);
        completeRecordReadOperations(ops, from, to, FAIL_UNKNOWN);
      }
    }
    return submitCount;
  }

  private Collection<String> getTopicsFromOps(List<O> ops, final int from, final int to) {
    HashSet<String> topicNames = new HashSet<>();
    for (var i = from; i < to; i++) {
      topicNames.add(ops.get(i).dstPath());
    }
    return topicNames;
  }

  private void submitNoop(final O op) {
    op.startRequest();
    completeOperation(op, SUCC);
  }

  @Override
  protected final int submit(final List<O> ops, final int from, final int to)
      throws IllegalStateException {
    val op = ops.get(from);
    val opType = op.type();
    if (OpType.READ.equals(opType)) {
      return submitBatchRecordRead(ops, from, to);
    } else {
      return submitEach(ops, from, to);
    }
  }

  @Override
  protected final int submit(final List<O> ops) throws IllegalStateException {
    return submit(ops, 0, ops.size());
  }

  int submitEach(final List<O> ops, final int from, final int to) {
    for (var i = from; i < to; i++) {
      if (!submit(ops.get(i))) {
        return i - from;
      }
    }
    return to - from;
  }

  String nextEndpointAddr() {
    return endpointAddrs[rrc.getAndIncrement() % endpointAddrs.length];
  }

  @Override
  protected boolean prepare(final O operation) {
    super.prepare(operation);
    var endpointAddr = operation.nodeAddr();
    if (endpointAddr == null) {
      endpointAddr = nextEndpointAddr() + ":" + this.nodePort;
      operation.nodeAddr(endpointAddr);
    }
    return true;
  }

  @Override
  protected String requestNewPath(String path) {
    throw new AssertionError("Should not be invoked");
  }

  @Override
  protected String requestNewAuthToken(Credential credential) {

    throw new AssertionError("Should not be invoked");
  }

  @Override
  public List<I> list(
      final ItemFactory<I> itemFactory,
      final String path,
      final String prefix,
      final int idRadix,
      final I lastPrevItem,
      final int count)
      throws IOException {

    if (listWasCalled) {
      throw new EOFException();
    }

    val buff = new ArrayList<I>(1);
    buff.add(itemFactory.getItem(path + prefix, 0, 0));

    listWasCalled = true;
    return buff;
  }

  @Override
  public void adjustIoBuffers(long avgTransferSize, OpType opType) {}
}
