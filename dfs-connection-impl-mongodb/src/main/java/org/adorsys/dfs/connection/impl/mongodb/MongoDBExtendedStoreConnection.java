package org.adorsys.dfs.connection.impl.mongodb;

import org.adorsys.dfs.connection.api.types.BucketPathEncryptionPassword;
import org.adorsys.dfs.connection.api.types.connection.MongoURI;
import org.adorsys.dfs.connection.api.types.properties.BucketPathEncryptionFilenameOnly;
import org.adorsys.dfs.connection.api.types.properties.MongoConnectionProperties;
import org.adorsys.dfs.connection.impl.pathencryption.BucketPathEncryptingExtendedStoreConnection;

/**
 * Created by peter on 27.09.18.
 */
public class MongoDBExtendedStoreConnection extends BucketPathEncryptingExtendedStoreConnection {
    public MongoDBExtendedStoreConnection(MongoConnectionProperties properties) {
        this(properties.getMongoURI(),
                properties.getBucketPathEncryptionPassword(),
                properties.getBucketPathEncryptionFilenameOnly());
    }

    public MongoDBExtendedStoreConnection(
            MongoURI mongoURI,
            BucketPathEncryptionPassword bucketPathEncryptionPassword,
            BucketPathEncryptionFilenameOnly bucketPathEncryptionFilenameOnly) {
        super(new RealMongoDBExtendedStoreConnection(mongoURI), bucketPathEncryptionPassword, bucketPathEncryptionFilenameOnly);
    }
}
