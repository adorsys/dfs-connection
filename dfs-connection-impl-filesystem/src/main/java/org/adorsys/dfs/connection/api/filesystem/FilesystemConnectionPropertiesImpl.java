package org.adorsys.dfs.connection.api.filesystem;

import org.adorsys.dfs.connection.api.types.connection.FilesystemRootBucketName;
import org.adorsys.dfs.connection.api.types.properties.ConnectionPropertiesImpl;
import org.adorsys.dfs.connection.api.types.properties.FilesystemConnectionProperties;

/**
 * Created by peter on 04.10.18.
 */
public class FilesystemConnectionPropertiesImpl extends ConnectionPropertiesImpl implements FilesystemConnectionProperties {
    private FilesystemRootBucketName filesystemRootBucketName = defaultBasedirectory;

    public FilesystemConnectionPropertiesImpl() {}

    public FilesystemConnectionPropertiesImpl(FilesystemConnectionProperties source) {
        super(source);
        filesystemRootBucketName = source.getFilesystemRootBucketName();
    }

    public FilesystemRootBucketName getFilesystemRootBucketName() {
        return filesystemRootBucketName;
    }

    public void setFilesystemRootBucketName(FilesystemRootBucketName filesystemRootBucketName) {
        this.filesystemRootBucketName = filesystemRootBucketName;
    }
}
