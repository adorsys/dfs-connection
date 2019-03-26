package org.adorsys.dfs.connection.api.filesystem;


import org.adorsys.dfs.connection.api.types.BucketPathEncryptionPassword;
import org.adorsys.dfs.connection.api.types.connection.FilesystemRootBucketName;
import org.adorsys.dfs.connection.api.types.properties.BucketPathEncryptionFilenameOnly;
import org.adorsys.dfs.connection.api.types.properties.FilesystemConnectionProperties;
import org.adorsys.dfs.connection.impl.pathencryption.BucketPathEncryptingExtendedStoreConnection;

/**
 * Created by peter on 27.09.18.
 */
public class FileSystemExtendedStorageConnection extends BucketPathEncryptingExtendedStoreConnection {
    public FileSystemExtendedStorageConnection(FilesystemConnectionProperties properties) {
        this(
                properties.getFilesystemRootBucketName(),
                properties.getBucketPathEncryptionPassword(),
                properties.getBucketPathEncryptionFilenameOnly());
    }

    public FileSystemExtendedStorageConnection(
            FilesystemRootBucketName basedir,
            BucketPathEncryptionPassword bucketPathEncryptionPassword,
            BucketPathEncryptionFilenameOnly bucketPathEncryptionFilenameOnly) {
        super(new RealFileSystemExtendedStorageConnection(basedir), bucketPathEncryptionPassword, bucketPathEncryptionFilenameOnly);
    }
}