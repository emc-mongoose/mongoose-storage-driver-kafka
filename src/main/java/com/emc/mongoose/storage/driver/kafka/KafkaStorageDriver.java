package com.emc.mongoose.storage.driver.kafka;

import static com.emc.mongoose.base.item.op.Operation.Status.SUCC;

import com.emc.mongoose.base.config.IllegalConfigurationException;
import com.emc.mongoose.base.data.DataInput;
import com.emc.mongoose.base.item.Item;
import com.emc.mongoose.base.item.ItemFactory;
import com.emc.mongoose.base.item.op.OpType;
import com.emc.mongoose.base.item.op.Operation;
import com.emc.mongoose.base.item.op.data.DataOperation;
import com.emc.mongoose.base.item.op.path.PathOperation;
import com.emc.mongoose.base.logging.LogUtil;
import com.emc.mongoose.base.storage.Credential;
import com.emc.mongoose.storage.driver.coop.CoopStorageDriverBase;
import com.github.akurilov.confuse.Config;
import lombok.val;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.logging.log4j.Level;

import java.io.EOFException;
import java.io.IOException;
import java.util.*;

import static com.emc.mongoose.base.Exceptions.throwUncheckedIfInterrupted;
import static com.emc.mongoose.base.item.op.Operation.Status.FAIL_UNKNOWN;
import static com.emc.mongoose.base.item.op.Operation.Status.INTERRUPTED;
import static com.emc.mongoose.base.item.op.Operation.Status.FAIL_IO;

public class KafkaStorageDriver<I extends Item, O extends Operation<I>>
    extends CoopStorageDriverBase<I, O> {

  private volatile boolean listWasCalled = false;

  public KafkaStorageDriver(
      String testStepId,
      DataInput dataInput,
      Config storageConfig,
      boolean verifyFlag,
      int batchSize)
      throws IllegalConfigurationException {
    super(testStepId, dataInput, storageConfig, verifyFlag, batchSize);
  }

  private final AdminClient adminClient = AdminClient.create(new Properties()); // will be in constructor

  @Override
  protected final boolean submit(final O op) throws IllegalStateException {
    if (concurrencyThrottle.tryAcquire()) {
      final var opType = op.type();
      if (opType.equals(OpType.NOOP)) {
        submitNoop(op);
      }
      if (op instanceof DataOperation) {
        submitRecordOperation((DataOperation) op, opType);
      } else if (op instanceof PathOperation) {
        submitTopicOperation((PathOperation) op, opType);
      } else {
        throw new AssertionError("storage driver doesn't support the token operations");
      }
    }
    return true;
  }

  private void submitRecordOperation(DataOperation op, OpType opType) {
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
        throw new AssertionError("Not implemented");
      default:
        throw new AssertionError("Not implemented");
    }
  }

  private void submitRecordDeleteOperation() {}

  private void submitRecordReadOperation() {}

  private void submitRecordCreateOperation() {}

  private void submitTopicOperation(PathOperation op, OpType opType) {
    switch (opType) {
      case CREATE:
        submitTopicCreateOperation(op);
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

  private void submitTopicCreateOperation(final PathOperation op) {

    final String topicName = op.item().name();
    try {
      val newTopic = new NewTopic(topicName, 1, (short) 1);
      val createTopicsResult = adminClient.createTopics(Collections.singleton(newTopic));
      val createTopicFuture = createTopicsResult.values().get(topicName)
              .whenComplete((result, thrown) -> handleTopicCreate(op, topicName, thrown));
    } catch (final NullPointerException e) {
      if (!isStarted()) {
        completeOperation((O) op, INTERRUPTED);
      } else {
        completeFailedOperation((O) op, e);
      }
    } catch (final Throwable thrown) {
      throwUncheckedIfInterrupted(thrown);
      completeFailedOperation((O) op, thrown);
    }
  }

  void handleTopicCreate(final PathOperation op, final String topicName, final Throwable exception) {
    if (exception != null) {
      if (!(exception.getCause() instanceof TopicExistsException)) {
        LogUtil.exception(Level.DEBUG, exception, "{}: the topic already exists", stepId);
        completeOperation((O) op, FAIL_IO);
      }
      completeFailedOperation((O) op, exception);
    } else {
      //topicCache.put(topicName, newTopic)
        completeOperation((O) op, SUCC);
    }
  }

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

  boolean completeFailedOperation(final O op, final Throwable thrown) {
    LogUtil.exception(Level.DEBUG, thrown, "{}: unexpected load operation failure", stepId);
    return completeOperation(op, FAIL_UNKNOWN);
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
