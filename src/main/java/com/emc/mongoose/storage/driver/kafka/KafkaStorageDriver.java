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
import com.github.akurilov.confuse.Config;
import java.io.IOException;
import java.util.List;

public class KafkaStorageDriver<I extends Item, O extends Operation<I>>
    extends CoopStorageDriverBase<I, O> {

  protected KafkaStorageDriver(
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
      final OpType opType = op.type();
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
    final int opsCount = ops.size();
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