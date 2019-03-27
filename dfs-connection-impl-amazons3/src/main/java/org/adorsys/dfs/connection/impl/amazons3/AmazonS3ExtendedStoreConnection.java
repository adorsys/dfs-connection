package org.adorsys.dfs.connection.impl.amazons3;


import org.adorsys.dfs.connection.api.types.BucketPathEncryptionPassword;
import org.adorsys.dfs.connection.api.types.connection.AmazonS3AccessKey;
import org.adorsys.dfs.connection.api.types.connection.AmazonS3Region;
import org.adorsys.dfs.connection.api.types.connection.AmazonS3RootBucketName;
import org.adorsys.dfs.connection.api.types.connection.AmazonS3SecretKey;
import org.adorsys.dfs.connection.api.types.properties.AmazonS3ConnectionProperties;
import org.adorsys.dfs.connection.api.types.properties.BucketPathEncryptionFilenameOnly;
import org.adorsys.dfs.connection.impl.pathencryption.BucketPathEncryptingExtendedStoreConnection;

import java.net.URL;

/**
 * Created by peter on 27.09.18.
 */
public class AmazonS3ExtendedStoreConnection extends BucketPathEncryptingExtendedStoreConnection {
    public AmazonS3ExtendedStoreConnection(AmazonS3ConnectionProperties properties) {
        this(
                properties.getUrl(),
                properties.getAmazonS3AccessKey(),
                properties.getAmazonS3SecretKey(),
                properties.getAmazonS3Region(),
                properties.getAmazonS3RootBucketName(),
                properties.getBucketPathEncryptionPassword(),
                properties.getBucketPathEncryptionFilenameOnly()
        );
    }

    public AmazonS3ExtendedStoreConnection(
            URL url,
            AmazonS3AccessKey accessKey,
            AmazonS3SecretKey secretKey,
            AmazonS3Region anAmazonS3Region,
            AmazonS3RootBucketName anAmazonS3RootBucketName,
            BucketPathEncryptionPassword bucketPathEncryptionPassword,
            BucketPathEncryptionFilenameOnly bucketPathEncryptionFilenameOnly) {
        super(new RealAmazonS3ExtendedStoreConnection(url, accessKey, secretKey, anAmazonS3Region, anAmazonS3RootBucketName), bucketPathEncryptionPassword, bucketPathEncryptionFilenameOnly);
    }


    public void cleanDatabase() {
        ((RealAmazonS3ExtendedStoreConnection) super.extendedStoreConnection).cleanDatabase();
    }


    public void showDatabase() {
        ((RealAmazonS3ExtendedStoreConnection) super.extendedStoreConnection).showDatabase();
    }
}