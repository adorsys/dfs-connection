package org.adorsys.dfs.connection.api.filesystem.exceptions;

import org.adorsys.dfs.connection.api.exceptions.StorageConnectionException;

/**
 * Created by peter on 06.02.18 at 14:53.
 */
public class FolderIsAFileException extends StorageConnectionException {
    public FolderIsAFileException(String message) {
        super(message);
    }
}
