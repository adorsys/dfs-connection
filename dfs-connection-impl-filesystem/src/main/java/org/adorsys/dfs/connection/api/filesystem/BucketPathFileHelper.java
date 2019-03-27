package org.adorsys.dfs.connection.api.filesystem;


import org.adorsys.dfs.connection.api.complextypes.BucketDirectory;
import org.adorsys.dfs.connection.api.complextypes.BucketPath;
import org.adorsys.dfs.connection.api.domain.ObjectHandle;

import java.io.File;

/**
 * Created by peter on 21.02.18 at 19:35.
 */
public class BucketPathFileHelper {
    static public File getAsFile(BucketPath bucketPath, boolean absolute) {
        return getAsFile(bucketPath.getObjectHandle(), absolute);
    }

    static public File getAsFile(BucketDirectory bucketPath, boolean absolute) {
        return getAsFile(bucketPath.getObjectHandle(), absolute);
    }

    static public File getAsFile(ObjectHandle objectHandle, boolean absolute) {
        String container = objectHandle.getContainer();
        String name = objectHandle.getName();
        String fullpath = container + BucketPath.BUCKET_SEPARATOR + name;
        if (absolute) {
            fullpath = BucketPath.BUCKET_SEPARATOR + fullpath;
        }
        return new File(fullpath);
    }
}
