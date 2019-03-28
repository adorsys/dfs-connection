package de.adorsys.dfs.connection.impl.factory;

import de.adorsys.common.exceptions.BaseException;
import de.adorsys.dfs.connection.api.filesystem.FileSystemExtendedStorageConnection;
import de.adorsys.dfs.connection.api.service.api.ExtendedStoreConnection;
import de.adorsys.dfs.connection.api.types.properties.AmazonS3ConnectionProperties;
import de.adorsys.dfs.connection.api.types.properties.ConnectionProperties;
import de.adorsys.dfs.connection.api.types.properties.FilesystemConnectionProperties;
import de.adorsys.dfs.connection.api.types.properties.MongoConnectionProperties;
import de.adorsys.dfs.connection.impl.amazons3.AmazonS3ExtendedStoreConnection;
import de.adorsys.dfs.connection.impl.mongodb.MongoDBExtendedStoreConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by peter on 15.03.18 at 11:38.
 */
public class ExtendedStoreConnectionFactory {
    private final static Logger LOGGER = LoggerFactory.getLogger(ExtendedStoreConnectionFactory.class);
    private static ConnectionProperties properties = null;

    public static ExtendedStoreConnection get(ConnectionProperties properties) {
        if (properties instanceof MongoConnectionProperties) {
            return new MongoDBExtendedStoreConnection((MongoConnectionProperties) properties);
        }
        if (properties instanceof AmazonS3ConnectionProperties) {
            return new AmazonS3ExtendedStoreConnection((AmazonS3ConnectionProperties) properties);
        }
        if (properties instanceof FilesystemConnectionProperties) {
            return new FileSystemExtendedStorageConnection((FilesystemConnectionProperties) properties);
        }
        throw new BaseException("Properties of unknown type: " + properties.getClass().getName());
    }

    public static ExtendedStoreConnection get() {
        if (properties == null) {
            properties = new ReadArguments().readEnvironment();
        }
        return get(properties);
    }

    public static void reset() {
        properties = null;
    }

    /**
     * @param args
     * @return die Argumente, die nicht verwertet werden konnten
     */
    public static String[] readArguments(String[] args) {
        ReadArguments.ArgsAndProperties argsAndProperties = new ReadArguments().readArguments(args);
        properties = argsAndProperties.properties;
        return argsAndProperties.remainingArgs;
    }
}