package com.emc.mongoose.storage.driver.kafka.cache;

import java.util.Properties;
import lombok.Value;
import org.apache.kafka.clients.admin.AdminClient;

@Value
public final class AdminClientCreateFunctionImpl implements AdminClientCreateFunction {

  Properties properties;

  @Override
  public final AdminClient apply(final String name) {
    properties.setProperty("group.id", name);
    return AdminClient.create(properties);
  }
}
