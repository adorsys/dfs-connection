package de.adorsys.dfs.connection.api.service.api;

import de.adorsys.dfs.connection.api.complextypes.BucketDirectory;
import de.adorsys.dfs.connection.api.complextypes.BucketPath;
import de.adorsys.dfs.connection.api.domain.Payload;
import de.adorsys.dfs.connection.api.domain.PayloadStream;
import de.adorsys.dfs.connection.api.types.ExtendedStoreConnectionType;
import de.adorsys.dfs.connection.api.types.ListRecursiveFlag;

import java.util.List;

public interface DFSConnection {

    void putBlob(BucketPath bucketPath, Payload payload);
    Payload getBlob(BucketPath bucketPath);

    void putBlobStream(BucketPath bucketPath, PayloadStream payloadStream);

    PayloadStream getBlobStream(BucketPath bucketPath);

    boolean blobExists(BucketPath bucketPath);

    void removeBlob(BucketPath bucketPath);
    void removeBlobFolder(BucketDirectory bucketDirectory);

    void createContainer(BucketDirectory bucketDirectory);

    boolean containerExists(BucketDirectory bucketDirectory);

    void deleteContainer(BucketDirectory bucketDirectory);

    List<BucketPath> list(BucketDirectory bucketDirectory, ListRecursiveFlag listRecursiveFlag);

    List<BucketDirectory> listAllBuckets();

    ExtendedStoreConnectionType getType();
}
