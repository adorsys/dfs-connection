package org.adorsys.dfs.connection.api.exceptions;

import org.adorsys.common.exceptions.BaseException;

/**
 * Created by peter on 26.09.18.
 */
public class PathEncryptionException extends BaseException {
    public PathEncryptionException(String path, Exception origException) {
        super("Can not encrypt " + path, origException);
    }
}
