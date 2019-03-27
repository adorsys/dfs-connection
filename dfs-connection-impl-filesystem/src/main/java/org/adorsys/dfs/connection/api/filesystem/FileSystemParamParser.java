package org.adorsys.dfs.connection.api.filesystem;

import org.adorsys.dfs.connection.api.types.connection.FilesystemRootBucketName;
import org.adorsys.dfs.connection.api.types.properties.FilesystemConnectionProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by peter on 13.04.18 at 19:19.
 */
public class FileSystemParamParser {
    private final static Logger LOGGER = LoggerFactory.getLogger(FileSystemParamParser.class);

    public static FilesystemConnectionProperties getProperties(String params) {
        LOGGER.debug("parse:" + params);
        FilesystemConnectionPropertiesImpl properties = new FilesystemConnectionPropertiesImpl();
        if (params.length() > 0) {
            properties.setFilesystemRootBucketName(new FilesystemRootBucketName(params));
        }
        return properties;
    }
}
