package com.emc.mongoose.storage.driver.kafka;

import static com.emc.mongoose.base.item.op.Operation.Status.INTERRUPTED;
import static com.emc.mongoose.base.item.op.Operation.Status.SUCC;

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
import com.emc.mongoose.storage.driver.kafka.cache.AdminClientCreateFunction;
import com.emc.mongoose.storage.driver.kafka.cache.AdminClientCreateFunctionImpl;
import com.github.akurilov.confuse.Config;
import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class KafkaStorageDriver<I extends Item, O extends Operation<I>>
    extends CoopStorageDriverBase<I, O> {

  private final Map<Properties, AdminClientCreateFunction> adminClientCreateFuncCache =
      new ConcurrentHashMap<>();
  private final Map<String, AdminClient> adminClientCache = new ConcurrentHashMap<>();

  public KafkaStorageDriver(
      String testStepId,
      DataInput dataInput,
      Config storageConfig,
      boolean verifyFlag,
      int batchSize)
      throws IllegalConfigurationException {
    super(testStepId, dataInput, storageConfig, verifyFlag, batchSize);
  }

  @Override
  protected final boolean submit(final O op) throws IllegalStateException {
    if (concurrencyThrottle.tryAcquire()) {
      final var opType = op.type();
      if (opType.equals(OpType.NOOP)) {
        submitNoop(op);
      }
      var nodeAddr = op.nodeAddr();
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

  private void submitRecordOperation(DataOperation op, OpType opType, final String nodeAddr) {
    switch (opType) {
      case CREATE:
        submitRecordCreateOperation();
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
        submitRecordsListingOperation(op, nodeAddr);
      default:
        throw new AssertionError("Not implemented");
    }
  }

  private void submitRecordDeleteOperation() {}

  private void submitRecordReadOperation() {}

  private void submitRecordCreateOperation() {}

  private void submitRecordsListingOperation(final DataOperation recordOp, final String nodeAddr) {
    try {
      var adminConfig =
          adminClientCreateFuncCache.computeIfAbsent(
              createAdminConfig(nodeAddr), AdminClientCreateFunctionImpl::new);
      var adminClient = adminClientCache.computeIfAbsent(nodeAddr, adminConfig);
      var recordItem = recordOp.item();
      var topicName = recordItem.name();

      Properties props = new Properties();
      props.put("bootstrap.servers", "localhost:9092");

      var topics = adminClient.listTopics().names().get();
      if (adminClient.listTopics().names().get().contains(topicName)) {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        var consumerRecords = consumer.poll(Duration.ofSeconds(10));
        var records = consumerRecords.records(topicName);
        recordOp.startRequest();
        completeOperation((O) recordItem, SUCC);
      } else {
        completeOperation((O) recordItem, INTERRUPTED);
      }

      recordOp.startRequest();
    } catch (final NullPointerException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
  }

  private Properties createAdminConfig(String nodeAddr) {
    var adminConfig = new Properties();
    adminConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, nodeAddr);
    return adminConfig;
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

  private void submitNoop(final O op) {
    op.startRequest();
    completeOperation(op, SUCC);
  }

  private boolean completeOperation(final O op, final Operation.Status status) {
    concurrencyThrottle.release();
    op.status(status);
    op.finishRequest();
    op.startResponse();
    op.finishResponse();
    return handleCompleted(op);
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
  protected final int submit(final List<O> ops) throws IllegalStateException {
    final var opsCount = ops.size();
    for (var i = 0; i < opsCount; i++) {
      if (!submit(ops.get(i))) {
        return i;
      }
    }
    return opsCount;
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
      ItemFactory<I> itemFactory,
      String path,
      String prefix,
      int idRadix,
      I lastPrevItem,
      int count)
      throws IOException {
    return null;
  }

  @Override
  public void adjustIoBuffers(long avgTransferSize, OpType opType) {}
}
