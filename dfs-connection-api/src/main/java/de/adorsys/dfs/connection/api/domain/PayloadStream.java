package de.adorsys.dfs.connection.api.domain;

import java.io.Closeable;
import java.io.InputStream;

/**
 * Created by peter on 05.03.18 at 08:33.
 */
public interface PayloadStream extends Closeable {
    /**
     * returns the inputstream of the data. The receiver is responsible for closing the stream
     */
    InputStream openStream();
}
