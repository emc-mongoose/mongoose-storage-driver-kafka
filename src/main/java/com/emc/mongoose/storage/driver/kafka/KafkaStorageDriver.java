package com.emc.mongoose.storage.driver.kafka;

import static com.emc.mongoose.base.Exceptions.throwUncheckedIfInterrupted;
import static com.emc.mongoose.base.item.op.Operation.Status.*;
import static com.github.akurilov.commons.io.el.ExpressionInput.ASYNC_MARKER;
import static com.github.akurilov.commons.io.el.ExpressionInput.INIT_MARKER;
import static com.github.akurilov.commons.io.el.ExpressionInput.SYNC_MARKER;
import static com.github.akurilov.commons.lang.Exceptions.throwUnchecked;

import com.emc.mongoose.base.config.IllegalConfigurationException;
import com.emc.mongoose.base.data.DataInput;
import com.emc.mongoose.base.item.DataItem;
import com.emc.mongoose.base.item.Item;
import com.emc.mongoose.base.item.ItemFactory;
import com.emc.mongoose.base.item.op.OpType;
import com.emc.mongoose.base.item.op.Operation;
import com.emc.mongoose.base.item.op.Operation.Status;
import com.emc.mongoose.base.item.op.data.DataOperation;
import com.emc.mongoose.base.item.op.path.PathOperation;
import com.emc.mongoose.base.logging.LogContextThreadFactory;
import com.emc.mongoose.base.logging.LogUtil;
import com.emc.mongoose.base.storage.Credential;
import com.emc.mongoose.storage.driver.kafka.cache.AdminClientCreateFunctionImpl;
import com.emc.mongoose.storage.driver.kafka.cache.ProducerCreateFunctionImpl;
import com.emc.mongoose.storage.driver.kafka.cache.TopicCreateFunction;
import com.emc.mongoose.storage.driver.kafka.cache.TopicCreateFunctionImpl;
import com.emc.mongoose.storage.driver.preempt.PreemptStorageDriverBase;
import com.github.akurilov.confuse.Config;
import java.io.EOFException;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.val;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.Level;

public class KafkaStorageDriver<I extends Item, O extends Operation<I>>
    extends PreemptStorageDriverBase<I, O> {

  private final String[] endpointAddrs;
  private final boolean useKey;
  private final int requestSizeLimit;
  private final int batchSize;
  private final int sndBuf;
  private final int rcvBuf;
  private final int linger;
  private final long buffer;
  private final String compression;
  private final Duration readTimeout;
  private final Semaphore concurrencyThrottle;
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
  protected final Map<String, String> sharedHeaders = new HashMap<>();
  protected final Map<String, String> dynamicHeaders = new HashMap<>();
  private volatile boolean listWasCalled = false;

  public KafkaStorageDriver(
      String testStepId,
      DataInput dataInput,
      Config storageConfig,
      boolean verifyFlag,
      int batchSize)
      throws IllegalConfigurationException {
    super(testStepId, dataInput, storageConfig, verifyFlag);
    val driverConfig = storageConfig.configVal("driver");
    val createConfig = driverConfig.configVal("create");
    val headersMap = createConfig.<String>mapVal("headers");
    if (!headersMap.isEmpty()) {
      for (final var header : headersMap.entrySet()) {
        final var headerKey = header.getKey();
        final var headerValue = header.getValue();
        if (headerKey.contains(ASYNC_MARKER)
            || headerKey.contains(SYNC_MARKER)
            || headerKey.contains(INIT_MARKER)
            || headerValue.contains(ASYNC_MARKER)
            || headerValue.contains(SYNC_MARKER)
            || headerValue.contains(INIT_MARKER)) {
          dynamicHeaders.put(headerKey, headerValue);
        } else {
          sharedHeaders.put(headerKey, headerValue);
        }
      }
    }
    this.useKey = createConfig.boolVal("key-enabled");
    this.requestSizeLimit = driverConfig.intVal("request-size");
    this.batchSize = batchSize;
    val netConfig = storageConfig.configVal("net");
    this.buffer = driverConfig.longVal("buffer-memory");
    this.compression = driverConfig.stringVal("compression-type");
    val nodeConfig = netConfig.configVal("node");
    val nodePort = nodeConfig.intVal("port");
    val endpointAddrList = nodeConfig.listVal("addrs");
    this.endpointAddrs = endpointAddrList.toArray(new String[endpointAddrList.size()]);
    for (var i = 0; i < endpointAddrs.length; i++) {
      if (!endpointAddrs[i].contains(":")) {
        endpointAddrs[i] = endpointAddrs[i] + ":" + nodePort;
      }
    }
    this.sndBuf = netConfig.intVal("sndBuf");
    this.rcvBuf = netConfig.intVal("rcvBuf");
    this.linger = netConfig.intVal("linger");
    val readTimeoutMillis = driverConfig.longVal("read-timeoutMillis");
    this.readTimeout =
        readTimeoutMillis > 0
            ? Duration.ofMillis(readTimeoutMillis)
            : Duration.ofDays(Long.MAX_VALUE);
    this.concurrencyThrottle =
        new Semaphore(concurrencyLimit > 0 ? concurrencyLimit : Integer.MAX_VALUE);
    this.requestAuthTokenFunc = null;
    this.requestNewPathFunc = null;
  }

  @Override
  protected ThreadFactory ioWorkerThreadFactory() {
    return new LogContextThreadFactory("io_worker_" + stepId, true);
  }

  /** @return true if the load operations are about the Kafka records, false otherwise */
  @Override
  protected final boolean isBatch(final List<O> ops, final int from, final int to) {
    return true;
  }

  @Override
  protected final void execute(final O op) {
    val opType = op.type();
    if (op instanceof DataOperation) {
      switch (opType) {
        case NOOP:
          noop(List.of(op));
          break;
        case CREATE:
          produceRecords(List.of(op));
          break;
        case READ:
          consumeRecords(List.of(op));
          break;
        default:
          throw new AssertionError("Unsupported records operation type: " + opType);
      }
    } else if (op instanceof PathOperation) {
      switch (opType) {
        case NOOP:
          noop(List.of(op));
          break;
        case CREATE:
          createTopics(List.of(op));
          break;
        case DELETE:
          deleteTopics(List.of(op));
          break;
        case LIST:
          throw new AssertionError("Unsupported topics operation type: " + opType);
      }
    } else {
      throw new AssertionError("Unsupported operation class: " + op.getClass());
    }
  }

  /** Batch mode branch */
  @Override
  protected final void execute(final List<O> ops) throws IllegalStateException {
    val op = ops.get(0);
    val opType = op.type();
    if (op instanceof DataOperation) {
      switch (opType) {
        case NOOP:
          noop(ops);
          break;
        case CREATE:
          produceRecords(ops);
          break;
        case READ:
          consumeRecords(ops);
          break;
        default:
          throw new AssertionError("Unsupported record operation type: " + opType);
      }
    } else if (op instanceof PathOperation) {
      switch (opType) {
        case NOOP:
          noop(List.of(op));
          break;
        case CREATE:
          createTopics(ops);
          break;
        case DELETE:
          deleteTopics(ops);
          break;
        case LIST:
          throw new AssertionError("Unsupported topics operation type: " + opType);
      }
    } else {
      throw new AssertionError("Unsupported operation class: " + op.getClass());
    }
  }

  void noop(final List<O> ops) {
    try {
      concurrencyThrottle.acquire();
    } catch (final InterruptedException e) {
      throwUnchecked(e);
    }
    var op = ops.get(0);
    if (op instanceof DataOperation) {
      for (var i = 0; i < ops.size(); i++) {
        op = ops.get(i);
        op.startRequest();
        op.finishRequest();
        op.startResponse();
        op.finishResponse();
        try {
          val dataOp = (DataOperation) op;
          dataOp.countBytesDone(dataOp.item().size());
        } catch (final IOException ignored) {
        }
      }
    } else {
      for (var i = 0; i < ops.size(); i++) {
        op = ops.get(i);
        op.startRequest();
        op.finishRequest();
        op.startResponse();
        op.finishResponse();
      }
    }
    concurrencyThrottle.release();
    completeOperations(ops, SUCC);
  }

  void completeOperations(final List<? extends O> ops, final Status status) {
    var op = (O) null;
    for (var i = 0; i < ops.size(); i++) {
      op = ops.get(i);
      op.status(status);
      handleCompleted(op);
    }
  }

  void produceRecords(final List<O> recOps) {
    val nodeAddr = recOps.get(0).nodeAddr();
    try {
      val config = configCache.computeIfAbsent(nodeAddr, this::createConfig);
      val producerConfig =
          producerCreateFuncCache.computeIfAbsent(config, ProducerCreateFunctionImpl::new);
      val producer = producerCache.computeIfAbsent(nodeAddr, producerConfig);
      val adminConfig =
          adminClientCreateFuncCache.computeIfAbsent(config, AdminClientCreateFunctionImpl::new);
      val adminClient = adminClientCache.computeIfAbsent(nodeAddr, adminConfig);
      var topicName = (String) null;
      var topicCreateFunc = (TopicCreateFunction) null;
      for (var i = 0; i < recOps.size(); i++) {
        val recOp = (DataOperation) recOps.get(i);
        // create the new topic if necessary
        topicName = recOp.dstPath();
        topicCreateFunc =
            topicCreateFuncCache.computeIfAbsent(adminClient, TopicCreateFunctionImpl::new);
        topicCache.computeIfAbsent(topicName, topicCreateFunc);
        // create the corresponding producer key and send it
        val recItem = recOp.item();
        val producerKey = useKey ? recItem.name() : null;
        val producerRecord = new ProducerRecord<String, DataItem>(topicName, producerKey, recItem);
        val recSize = recItem.size();
        concurrencyThrottle.acquire();
        recOp.startRequest();
        producer.send(producerRecord, (md, e) -> handleRecordProduce(recOp, recSize, e));
        recOp.finishRequest();
      }
      producer.flush();
    } catch (final Throwable e) {
      throwUncheckedIfInterrupted(e);
      LogUtil.exception(Level.DEBUG, e, "Producing records failure");
      completeOperations(recOps, FAIL_UNKNOWN);
    }
  }

  void handleRecordProduce(final DataOperation recOp, final long recSize, final Throwable e) {
    recOp.startResponse();
    recOp.finishResponse();
    concurrencyThrottle.release();
    if (null == e) {
      recOp.countBytesDone(recSize);
      completeOperation((O) recOp, SUCC);
    } else {
      completeFailedOperation((O) recOp, e);
    }
  }

  void consumeRecords(final List<O> recOps) {
    val nodeAddr = recOps.get(0).nodeAddr();
    val opCount = recOps.size();
    int remainingOpCount = opCount;
    // each poll call may return less records than required, so poll in the loop until all required
    // records are done
    while (remainingOpCount > 0) {
      // it's necessary to use new consumer config every time to set the specific records count
      // limit
      val consumerConfig = createConsumerConfig(nodeAddr);
      // set the records count limit equal to the remaining read operations count
      consumerConfig.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, remainingOpCount);
      try (val consumer = new KafkaConsumer(consumerConfig)) {
        try {
          concurrencyThrottle.acquire();
          var recOp = (DataOperation) null;
          // mark the request time for all remaining operations
          for (var i = opCount - remainingOpCount; i < opCount; i++) {
            recOp = (DataOperation) recOps.get(i);
            recOp.startRequest();
            recOp.finishRequest();
          }
          val pollResult = consumer.poll(readTimeout);
          val recIter = pollResult.iterator();
          var rec = (ConsumerRecord) null;
          while (recIter.hasNext()) {
            rec = (ConsumerRecord) recIter.next();
            recOp = (DataOperation) recOps.get(opCount - remainingOpCount);
            // mark the response times and the transferred byte count
            recOp.startResponse();
            recOp.finishResponse();
            recOp.countBytesDone(rec.serializedValueSize());
            completeOperation((O) recOp, SUCC);
            remainingOpCount--;
          }
        } finally {
          concurrencyThrottle.release();
        }
      } catch (final Throwable e) {
        throwUncheckedIfInterrupted(e);
      }
    }
  }

  void createTopics(final List<O> topicOps) {}

  void deleteTopics(final List<O> topicOps) {}

  void completeOperation(final O op, final Status status) {
    op.status(status);
    handleCompleted(op);
  }

  void completeFailedOperation(final O op, final Throwable e) {
    LogUtil.exception(Level.DEBUG, e, "{}: operation failed: {}", stepId, op);
    completeOperation(op, FAIL_UNKNOWN);
  }

  Properties createConfig(String nodeAddr) {
    var producerConfig = new Properties();
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, nodeAddr);
    producerConfig.put(ProducerConfig.BATCH_SIZE_CONFIG, this.batchSize);
    producerConfig.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, this.requestSizeLimit);
    producerConfig.put(ProducerConfig.BUFFER_MEMORY_CONFIG, this.buffer);
    producerConfig.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compression);
    producerConfig.put(ProducerConfig.SEND_BUFFER_CONFIG, this.sndBuf);
    producerConfig.put(ProducerConfig.LINGER_MS_CONFIG, this.linger);
    producerConfig.put(ProducerConfig.RECEIVE_BUFFER_CONFIG, this.rcvBuf);
    try {
      producerConfig.put(
          ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
          Class.forName("org.apache.kafka.common.serialization.StringSerializer"));
      producerConfig.put(
          ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
          Class.forName("com.emc.mongoose.storage.driver.kafka.io.DataItemSerializer"));
    } catch (ClassNotFoundException e) {
      LogUtil.exception(Level.DEBUG, e, "{}: operation failed", stepId);
    }
    return producerConfig;
  }

  Properties createConsumerConfig(String nodeAddr) {
    var consumerConfig = new Properties();
    consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, nodeAddr);
    consumerConfig.put(ConsumerConfig.SEND_BUFFER_CONFIG, this.sndBuf);
    consumerConfig.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, this.rcvBuf);
    return consumerConfig;
  }

  String nextEndpointAddr() {
    return endpointAddrs[rrc.getAndIncrement() % endpointAddrs.length];
  }

  @Override
  protected boolean prepare(final O operation) {
    super.prepare(operation);
    var endpointAddr = operation.nodeAddr();
    if (endpointAddr == null) {
      operation.nodeAddr(nextEndpointAddr());
    }
    return true;
  }

  @Override
  protected String requestNewPath(String path) {
    throw new AssertionError("Should not be invoked");
  }

  @Override
  protected void doClose() throws IOException, IllegalStateException {
    super.doClose();
    configCache.clear();
    adminClientCreateFuncCache.clear();
    adminClientCache
        .values()
        .parallelStream()
        .forEach(adminClient -> adminClient.close(Duration.ofSeconds(10)));
    adminClientCache.clear();
    producerCreateFuncCache.clear();
    producerCache
        .values()
        .parallelStream()
        .forEach(producer -> producer.close(Duration.ofSeconds(10)));
    producerCache.clear();
    topicCreateFuncCache.clear();
    topicCache.clear();
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
