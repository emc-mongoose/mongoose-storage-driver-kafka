package com.emc.mongoose.storage.driver.kafka.cache;

import java.util.Properties;
import lombok.Value;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;

@Value
public final class AdminClientCreateFunctionImpl implements AdminClientCreateFunction {

  private Properties properties;

  @Override
  public final AdminClient apply(final String name) {
    properties.setProperty(AdminClientConfig.CLIENT_ID_CONFIG, name);
    return AdminClient.create(properties);
  }
}
