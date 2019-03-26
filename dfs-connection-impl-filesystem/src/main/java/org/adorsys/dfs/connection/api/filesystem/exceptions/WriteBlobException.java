package org.adorsys.dfs.connection.api.filesystem.exceptions;

import org.adorsys.dfs.connection.api.exceptions.StorageConnectionException;

/**
 * Created by peter on 06.02.18 at 15:08.
 */
public class WriteBlobException extends StorageConnectionException {
    public WriteBlobException(String message, Throwable cause) {
        super(message, cause);
    }
}
