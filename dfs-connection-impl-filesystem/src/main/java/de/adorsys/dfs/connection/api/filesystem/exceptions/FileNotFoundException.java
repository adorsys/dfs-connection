package de.adorsys.dfs.connection.api.filesystem.exceptions;

import de.adorsys.dfs.connection.api.exceptions.StorageConnectionException;

/**
 * Created by peter on 06.02.18 at 15:17.
 */
public class FileNotFoundException extends StorageConnectionException {
    public FileNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }
    public FileNotFoundException(String message) {
        super(message);
    }
}
