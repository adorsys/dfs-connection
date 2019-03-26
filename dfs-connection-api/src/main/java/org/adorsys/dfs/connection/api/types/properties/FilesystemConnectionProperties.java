package org.adorsys.dfs.connection.api.types.properties;

import org.adorsys.dfs.connection.api.types.connection.FilesystemRootBucketName;

/**
 * Created by peter on 04.10.18.
 */
public interface FilesystemConnectionProperties  extends ConnectionProperties {
    FilesystemRootBucketName defaultBasedirectory = new FilesystemRootBucketName("target/filesystemstorage");

    FilesystemRootBucketName getFilesystemRootBucketName();
}
