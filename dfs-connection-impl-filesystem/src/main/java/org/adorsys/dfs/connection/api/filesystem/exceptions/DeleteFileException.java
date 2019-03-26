package org.adorsys.dfs.connection.api.filesystem.exceptions;

import org.adorsys.dfs.connection.api.exceptions.StorageConnectionException;

/**
 * Created by peter on 06.02.18 at 16:22.
 */
public class DeleteFileException extends StorageConnectionException {
    public DeleteFileException(String message, Throwable cause) {
        super(message, cause);
    }
}
