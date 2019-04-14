package com.emc.mongoose.storage.driver.kafka;

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
import com.emc.mongoose.storage.driver.kafka.cache.AdminClientCreateFunction;
import com.emc.mongoose.storage.driver.kafka.cache.AdminClientCreateFunctionImpl;
import com.github.akurilov.confuse.Config;
import lombok.val;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.logging.log4j.Level;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

import static com.emc.mongoose.base.Exceptions.throwUncheckedIfInterrupted;
import static com.emc.mongoose.base.item.op.Operation.Status.FAIL_UNKNOWN;
import static com.emc.mongoose.base.item.op.Operation.Status.INTERRUPTED;

public class KafkaStorageDriver<I extends Item, O extends Operation<I>>
    extends CoopStorageDriverBase<I, O> {

  public KafkaStorageDriver(
      String testStepId,
      DataInput dataInput,
      Config storageConfig,
      boolean verifyFlag,
      int batchSize)
      throws IllegalConfigurationException {
    super(testStepId, dataInput, storageConfig, verifyFlag, batchSize);
  }

  private final Map<Properties, AdminClientCreateFunction> adminClientCreateFuncCache =
          new ConcurrentHashMap<>();
  private final Map<String, AdminClient> adminClientCache = new ConcurrentHashMap<>();

  @Override
  protected final boolean submit(final O op) throws IllegalStateException {
    if (concurrencyThrottle.tryAcquire()) {
      final var opType = op.type();
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

    final String topicName = op.dstPath();
    try {
      val properties = new Properties();
      try {
        properties.load(new FileReader(new File("kafka.properties")));
      } catch (IOException e) {
        e.printStackTrace();
      }
      val adminClientCreateFunc = adminClientCreateFuncCache.computeIfAbsent(properties,
              AdminClientCreateFunctionImpl::new);
      val adminClient = adminClientCache.computeIfAbsent("kafka", adminClientCreateFunc);
      val newTopic = new NewTopic(topicName, 1, (short) 1);
      final CreateTopicsResult createTopicsResult = adminClient.createTopics(Collections.singleton(newTopic));
      createTopicsResult.values().get(topicName).get();
    } catch (InterruptedException | ExecutionException e) {
      if (!(e.getCause() instanceof TopicExistsException)) {
        // TopicExistsException - Swallow this exception, just means the topic already exists.
        completeFailedOperation((O) op, e);
      }
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

  private void submitTopicReadOperation() {}

  private void submitTopicDeleteOperation() {}

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
