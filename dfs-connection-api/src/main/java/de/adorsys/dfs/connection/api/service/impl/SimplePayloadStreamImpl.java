package de.adorsys.dfs.connection.api.service.impl;

import de.adorsys.dfs.connection.api.domain.PayloadStream;

import java.io.InputStream;

/**
 * Created by peter on 05.03.18 at 08:30.
 */
public class SimplePayloadStreamImpl implements PayloadStream {
    private InputStream inputStream;

    public SimplePayloadStreamImpl(InputStream inputStream) {
        this.inputStream = inputStream;
    }

    public SimplePayloadStreamImpl(PayloadStream payloadStream) {
        this(payloadStream.openStream());
    }

    @Override
    public InputStream openStream() {
        return inputStream;
    }


}
