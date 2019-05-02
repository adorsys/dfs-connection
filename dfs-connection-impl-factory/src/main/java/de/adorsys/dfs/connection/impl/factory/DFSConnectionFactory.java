package de.adorsys.dfs.connection.impl.factory;

import de.adorsys.common.exceptions.BaseException;
import de.adorsys.dfs.connection.api.filesystem.FileSystemDFSConnection;
import de.adorsys.dfs.connection.api.service.api.DFSConnection;
import de.adorsys.dfs.connection.api.types.properties.AmazonS3ConnectionProperties;
import de.adorsys.dfs.connection.api.types.properties.ConnectionProperties;
import de.adorsys.dfs.connection.api.types.properties.FilesystemConnectionProperties;
import de.adorsys.dfs.connection.impl.amazons3.AmazonS3DFSConnection;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by peter on 15.03.18 at 11:38.
 */
@Slf4j
public class DFSConnectionFactory {
    private final Logger LOGGER = LoggerFactory.getLogger(DFSConnectionFactory.class);
    private ConnectionProperties properties = null;

    public DFSConnectionFactory() {
        log.debug("create new DFSConnectionFactory");
    }

    public static DFSConnection get() {
        return new DFSConnectionFactory().getDFSConnection();
    }

    public static DFSConnection get(ConnectionProperties properties) {
        return new DFSConnectionFactory().getDFSConnection(properties);
    }

    public DFSConnection getDFSConnection(ConnectionProperties properties) {
        log.debug("call getDFSConnection");
        if (properties instanceof AmazonS3ConnectionProperties) {
            return new AmazonS3DFSConnection((AmazonS3ConnectionProperties) properties);
        }
        if (properties instanceof FilesystemConnectionProperties) {
            return new FileSystemDFSConnection((FilesystemConnectionProperties) properties);
        }
        throw new BaseException("Properties of unknown type: " + properties.getClass().getName());
    }

    public DFSConnection getDFSConnection() {
        if (properties == null) {
            properties = new ReadArguments().readEnvironment();
        }
        return getDFSConnection(properties);
    }


    /**
     * @param args
     * @return die Argumente, die nicht verwertet werden konnten
     */
    public String[] readArguments(String[] args) {
        ReadArguments.ArgsAndProperties argsAndProperties = new ReadArguments().readArguments(args);
        properties = argsAndProperties.properties;
        return argsAndProperties.remainingArgs;
    }
}
