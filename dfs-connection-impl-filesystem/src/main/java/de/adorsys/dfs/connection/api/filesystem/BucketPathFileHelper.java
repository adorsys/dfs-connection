package de.adorsys.dfs.connection.api.filesystem;


import de.adorsys.dfs.connection.api.complextypes.BucketDirectory;
import de.adorsys.dfs.connection.api.complextypes.BucketPath;

import java.io.File;

/**
 * Created by peter on 21.02.18 at 19:35.
 */
public class BucketPathFileHelper {
    static public File getAsFile(BucketPath bucketPath, boolean absolute) {
        return getAsFile(bucketPath.getValue(), absolute);
    }

    static public File getAsFile(BucketDirectory bucketPath, boolean absolute) {
        return getAsFile(bucketPath.getValue(), absolute);
    }

    static public File getAsFile(String path, boolean absolute) {
        if (absolute) {
            path = BucketPath.BUCKET_SEPARATOR + path;
        }
        return new File(path);
    }
}
