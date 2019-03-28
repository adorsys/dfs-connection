package de.adorsys.dfs.connection.api.types.properties;

import de.adorsys.dfs.connection.api.types.connection.MongoURI;

/**
 * Created by peter on 04.10.18.
 */
public interface MongoConnectionProperties extends ConnectionProperties {
    MongoURI getMongoURI();
}
