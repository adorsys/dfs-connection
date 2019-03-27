package org.adorsys.dfs.connection.api.service.api;

import org.adorsys.dfs.connection.api.complextypes.BucketDirectory;
import org.adorsys.dfs.connection.api.complextypes.BucketPath;
import org.adorsys.dfs.connection.api.domain.Payload;
import org.adorsys.dfs.connection.api.domain.PayloadStream;
import org.adorsys.dfs.connection.api.domain.StorageMetadata;
import org.adorsys.dfs.connection.api.types.ExtendedStoreConnectionType;
import org.adorsys.dfs.connection.api.types.ListRecursiveFlag;

import java.util.List;

public interface ExtendedStoreConnection {

    void putBlob(BucketPath bucketPath, Payload payload);
    Payload getBlob(BucketPath bucketPath);

    // Wenn die StorageMetadata bereits bekannt sind. Dann werden sie
    // direkt in die zurückgegebene Payload geschrieben und nicht explizit
    // ausgelesen
    Payload getBlob(BucketPath bucketPath, StorageMetadata storageMetadata);

    void putBlobStream(BucketPath bucketPath, PayloadStream payloadStream);

    PayloadStream getBlobStream(BucketPath bucketPath);
    // Wenn die StorageMetadata bereits bekannt sind. Dann werden sie
    // direkt in den zurückgegebenen PayloadStream geschrieben und nicht explizit
    // ausgelesen
    PayloadStream getBlobStream(BucketPath bucketPath, StorageMetadata storageMetadata);

    @Deprecated
    void putBlob(BucketPath bucketPath, byte[] bytes);

    StorageMetadata getStorageMetadata(BucketPath bucketPath);

    boolean blobExists(BucketPath bucketPath);

    void removeBlob(BucketPath bucketPath);
    void removeBlobFolder(BucketDirectory bucketDirectory);

    void createContainer(BucketDirectory bucketDirectory);

    boolean containerExists(BucketDirectory bucketDirectory);

    void deleteContainer(BucketDirectory bucketDirectory);

    List<StorageMetadata> list(BucketDirectory bucketDirectory, ListRecursiveFlag listRecursiveFlag);

    List<BucketDirectory> listAllBuckets();

    ExtendedStoreConnectionType getType();
}
