package de.adorsys.dfs.connection.api.service.impl;

import de.adorsys.common.exceptions.BaseException;
import de.adorsys.dfs.connection.api.domain.Payload;

public class SimplePayloadImpl implements Payload {
    private byte[] data = null;

    public SimplePayloadImpl(Payload payload) {
        this(payload.getData());
    }

    public SimplePayloadImpl(byte[] data) {
        if (data == null || data.length == 0) {
            throw new BaseException("Programming error, data must not be null");
        }
        if (data == null || data.length < 1) {
            throw new BaseException("Programming error, size must not be null or < 1");
        }
        this.data = data;
    }

    @Override
    public byte[] getData() {
        return data;
    }

}
