package com.emc.mongoose.storage.driver.kafka.io;

import com.emc.mongoose.base.item.DataItem;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.kafka.common.serialization.Serializer;

public class DataItemSerializer implements Serializer<DataItem> {

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {}

  @Override
  public byte[] serialize(String topic, DataItem dataItem) {
    try {
      final var dataItemSize = dataItem.size();
      if (Integer.MAX_VALUE < dataItemSize) {
        throw new IllegalArgumentException("Can't serialize the data item with size > 2^31 - 1");
      }
      final var dstBuff = ByteBuffer.allocate((int) dataItemSize);
      while (dstBuff.remaining() > 0) {
        dataItem.read(dstBuff);
      }
      dstBuff.flip();
      return dstBuff.array();
    } catch (final IOException e) {
      throw new AssertionError(e);
    }
  }

  @Override
  public void close() {}
}
