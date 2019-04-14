package com.emc.mongoose.storage.driver.kafka.cache;

import org.apache.kafka.clients.admin.AdminClient;

import java.util.function.Function;

/**
 * A function to create the admin client
 */
public interface AdminClientCreateFunction
        extends Function<String, AdminClient> {

    /**
     * @param name the name of adminClient
     * @return the created admin client
     */

    @Override
    AdminClient apply(final String name);
}