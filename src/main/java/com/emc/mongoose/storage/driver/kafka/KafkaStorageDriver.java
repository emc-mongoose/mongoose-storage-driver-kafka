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
import com.github.akurilov.commons.concurrent.ContextAwareThreadFactory;
import com.github.akurilov.confuse.Config;
import java.io.EOFException;
import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.val;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.ThreadContext;

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
  private final Duration recordOpTimeout;
  private final Semaphore concurrencyThrottle;
  private final AtomicInteger rrc = new AtomicInteger(0);
  private final Map<String, Properties> configCache = new ConcurrentHashMap<>();
  private final Map<Properties, AdminClientCreateFunctionImpl> adminClientCreateFuncCache =
      new ConcurrentHashMap<>();
  private final Map<String, AdminClient> adminClientCache = new ConcurrentHashMap<>();
  private final Map<Properties, ProducerCreateFunctionImpl> producerCreateFuncCache =
      new ConcurrentHashMap<>();
  private final ThreadLocal<Map<String, KafkaProducer>> threadLocalProducerCache =
      ThreadLocal.withInitial(ConcurrentHashMap::new);
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
    super(testStepId, dataInput, storageConfig, verifyFlag, batchSize);
    val driverConfig = storageConfig.configVal("driver");
    val recordConfig = driverConfig.configVal("record");
    val headersMap = recordConfig.<String>mapVal("headers");
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
    this.useKey = recordConfig.boolVal("key-enabled");
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
    val recordOpTimeoutMillis = recordConfig.longVal("timeoutMillis");
    this.recordOpTimeout =
        recordOpTimeoutMillis > 0
            ? Duration.ofMillis(recordOpTimeoutMillis)
            : Duration.ofDays(Long.MAX_VALUE);
    this.concurrencyThrottle =
        new Semaphore(concurrencyLimit > 0 ? concurrencyLimit : Integer.MAX_VALUE);
    this.requestAuthTokenFunc = null;
    this.requestNewPathFunc = null;
  }

  final class IoWorkerThreadFactory extends ContextAwareThreadFactory {

    public IoWorkerThreadFactory() {
      super("io_worker_" + stepId, true, ThreadContext.getContext());
    }

    @Override
    public final Thread newThread(final Runnable task) {
      return new IoWorkerThread(
          task,
          threadNamePrefix + "#" + threadNumber.incrementAndGet(),
          daemonFlag,
          exceptionHandler,
          threadContext);
    }

    final class IoWorkerThread extends LogContextThreadFactory.ContextAwareThread {

      public IoWorkerThread(
          final Runnable task,
          final String name,
          final boolean daemonFlag,
          final UncaughtExceptionHandler exceptionHandler,
          final Map<String, String> threadContext) {
        super(task, name, daemonFlag, exceptionHandler, threadContext);
      }

      @Override
      public final void interrupt() {
        val producerCache = threadLocalProducerCache.get();
        producerCache
            .values()
            .parallelStream()
            .forEach(producer -> producer.close(Duration.ofSeconds(10)));
        producerCache.clear();
        super.interrupt();
      }
    }
  }

  @Override
  protected ThreadFactory ioWorkerThreadFactory() {
    return new IoWorkerThreadFactory();
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
      val producerCache = threadLocalProducerCache.get();
      val producer = producerCache.computeIfAbsent(nodeAddr, producerConfig);
      val adminConfig =
          adminClientCreateFuncCache.computeIfAbsent(config, AdminClientCreateFunctionImpl::new);
      val adminClient = adminClientCache.computeIfAbsent(nodeAddr, adminConfig);
      var topicName = (String) null;
      var topicCreateFunc = (TopicCreateFunction) null;
      for (var i = 0; i < recOps.size(); i++) {
        val recOp = (DataOperation) recOps.get(i);
        // create the new topic if necessary
        topicName = recOp.dstPath().replaceAll("/", "");
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
        try {
          recOp.finishRequest();
        } catch (final IllegalStateException ignored) {
        }
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
      var recOp = (DataOperation) null;
      try (val consumer = new KafkaConsumer(consumerConfig)) {
        try {
          concurrencyThrottle.acquire();
          val topics = new HashSet<>();
          // mark the request time for all remaining operations
          for (var i = opCount - remainingOpCount; i < opCount; i++) {
            recOp = (DataOperation) recOps.get(i);
            recOp.startRequest();
            recOp.finishRequest();
            val topicName = recOp.srcPath().replaceAll("/", "");
            topics.add(topicName);
          }
          consumer.subscribe(topics);
          val pollResult = consumer.poll(recordOpTimeout);
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
        completeFailedOperation((O) recOp, e);
        throwUncheckedIfInterrupted(e);
      }
    }
  }

  void createTopics(final List<O> topicOps) {
    val nodeAddr = topicOps.get(0).nodeAddr();
    try {
      val config = configCache.computeIfAbsent(nodeAddr, this::createAdminClientConfig);
      val adminClientCreateFunc =
          adminClientCreateFuncCache.computeIfAbsent(config, AdminClientCreateFunctionImpl::new);
      val adminClient = adminClientCache.computeIfAbsent(nodeAddr, adminClientCreateFunc);
      val topicCollection = new ArrayList<NewTopic>();
      for (var i = 0; i < topicOps.size(); i++) {
        val topicOp = topicOps.get(i);
        val topicName = topicOp.item().name();
        val newTopic = new NewTopic(topicName, 1, (short) 1);
        topicCollection.add(newTopic);
      }
      concurrencyThrottle.acquire(topicOps.size());
      val createTopicsResultMap = adminClient.createTopics(topicCollection).values();
      for (var i = 0; i < topicOps.size(); i++) {
        hanldeTopicCreateOperation(topicOps.get(i), createTopicsResultMap);
      }
    } catch (final Throwable thrown) {
      throwUncheckedIfInterrupted(thrown);
      for (var i = 0; i < topicOps.size(); i++) {
        val topicOp = topicOps.get(i);
        completeFailedOperation(topicOp, thrown);
      }
      LogUtil.exception(
          Level.DEBUG,
          thrown,
          "{}: unexpected failure while trying to create {} records",
          stepId,
          topicOps.size());
    }
  }

  KafkaFuture.BiConsumer<Void, Throwable> handleTopicCreateFuture(final PathOperation topicOp) {
    val topicName = topicOp.item().name();
    KafkaFuture.BiConsumer<Void, Throwable> action =
        (aVoid, throwable) -> {
          if (throwable == null) {
            topicOp.startResponse();
            topicOp.finishResponse();
            completeOperation((O) topicOp, SUCC);
          } else {
            LogUtil.exception(
                Level.DEBUG, throwable, "{}: Failed to create topic \"{}\"", stepId, topicName);
            if (throwable instanceof TopicExistsException) {
              LogUtil.exception(
                  Level.DEBUG, throwable, "{}: Topic \"{}\" already exists", stepId, topicName);
            }
            completeOperation((O) topicOp, RESP_FAIL_UNKNOWN);
          }
          concurrencyThrottle.release();
        };
    return action;
  }

  void hanldeTopicCreateOperation(final O topicOp, final Map<String, KafkaFuture<Void>> resultMap) {
    val topicName = topicOp.item().name();
    topicOp.startRequest();
    val oneTopicResult = resultMap.get(topicName);
    oneTopicResult.whenComplete(handleTopicCreateFuture((PathOperation) topicOp));
    topicOp.finishRequest();
  }

  void deleteTopics(final List<O> topicOps) {
    val nodeAddr = topicOps.get(0).nodeAddr();
    try {
      val config = configCache.computeIfAbsent(nodeAddr, this::createAdminClientConfig);
      val adminClientCreateFunc =
          adminClientCreateFuncCache.computeIfAbsent(config, AdminClientCreateFunctionImpl::new);
      val adminClient = adminClientCache.computeIfAbsent(nodeAddr, adminClientCreateFunc);
      val topicNames = new ArrayList<String>();
      for (val topicOp : topicOps) {
        val topicName = topicOp.item().name();
        topicNames.add(topicName);
      }
      concurrencyThrottle.acquire(topicOps.size());
      val deleteTopicsResultMap = adminClient.deleteTopics(topicNames).values();
      for (val topicOp : topicOps) {
        hanldeTopicDeleteOperation(topicOp, deleteTopicsResultMap);
      }
    } catch (final Throwable thrown) {
      throwUncheckedIfInterrupted(thrown);
      for (val topicOp : topicOps) {
        completeFailedOperation(topicOp, thrown);
      }
      LogUtil.exception(
          Level.DEBUG,
          thrown,
          "{}: unexpected failure while trying to delete {} topics",
          stepId,
          topicOps.size());
    }
  }

  KafkaFuture.BiConsumer<Void, Throwable> handleTopicDeleteFuture(final PathOperation topicOp) {
    val topicName = topicOp.item().name();
    KafkaFuture.BiConsumer<Void, Throwable> action =
        (aVoid, throwable) -> {
          if (throwable == null) {
            topicOp.startResponse();
            topicOp.finishResponse();
            completeOperation((O) topicOp, SUCC);
          } else {
            LogUtil.exception(
                Level.DEBUG, throwable, "{}: Failed to delete topic \"{}\"", stepId, topicName);
            completeOperation((O) topicOp, RESP_FAIL_UNKNOWN);
          }
          concurrencyThrottle.release();
        };
    return action;
  }

  void hanldeTopicDeleteOperation(final O topicOp, final Map<String, KafkaFuture<Void>> resultMap) {
    val topicName = topicOp.item().name();
    topicOp.startRequest();
    val oneTopicResult = resultMap.get(topicName);
    oneTopicResult.whenComplete(handleTopicDeleteFuture((PathOperation) topicOp));
    topicOp.finishRequest();
  }

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
    consumerConfig.setProperty(
        ConsumerConfig.CLIENT_ID_CONFIG, String.valueOf(System.currentTimeMillis()));
    consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, nodeAddr);
    consumerConfig.put(ConsumerConfig.SEND_BUFFER_CONFIG, this.sndBuf);
    consumerConfig.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, this.rcvBuf);
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "group");
    consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    try {
      consumerConfig.put(
          ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
          Class.forName("org.apache.kafka.common.serialization.StringDeserializer"));
      consumerConfig.put(
          ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
          Class.forName("org.apache.kafka.common.serialization.ByteArrayDeserializer"));
    } catch (ClassNotFoundException e) {
      LogUtil.exception(Level.DEBUG, e, "{}: operation failed", stepId);
    }
    return consumerConfig;
  }

  Properties createAdminClientConfig(final String nodeAddr) {
    val adminClientConfig = new Properties();
    adminClientConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, nodeAddr);
    return adminClientConfig;
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
