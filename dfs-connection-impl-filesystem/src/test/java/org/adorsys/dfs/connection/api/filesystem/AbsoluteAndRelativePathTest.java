package org.adorsys.dfs.connection.api.filesystem;

import org.adorsys.common.exceptions.BaseExceptionHandler;
import org.adorsys.dfs.connection.api.complextypes.BucketDirectory;
import org.adorsys.dfs.connection.api.complextypes.BucketPath;
import org.adorsys.dfs.connection.api.service.impl.SimplePayloadImpl;
import org.adorsys.dfs.connection.api.service.impl.SimpleStorageMetadataImpl;
import org.adorsys.dfs.connection.api.types.connection.FilesystemRootBucketName;
import org.adorsys.dfs.connection.api.types.properties.BucketPathEncryptionFilenameOnly;
import org.adorsys.dfs.connection.api.types.properties.ConnectionProperties;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.UUID;

/**
 * Created by peter on 26.06.18 at 15:23.
 */
public class AbsoluteAndRelativePathTest {
    private final static Logger LOGGER = LoggerFactory.getLogger(AbsoluteAndRelativePathTest.class);

    @Test
    public void realtivePath() {
        try {
            String currentDir = new File(".").getCanonicalPath();
            String mydir = "target" + BucketPath.BUCKET_SEPARATOR + UUID.randomUUID().toString();
            String relativeDirAsAbsoluteDir = currentDir + BucketPath.BUCKET_SEPARATOR + mydir;
            LOGGER.debug("relative Dir to be created is (absoute):" + relativeDirAsAbsoluteDir);
            Assert.assertFalse(new File(relativeDirAsAbsoluteDir).exists());
            FileSystemExtendedStorageConnection con = new FileSystemExtendedStorageConnection(new FilesystemRootBucketName(mydir), ConnectionProperties.defaultEncryptionPassword, BucketPathEncryptionFilenameOnly.FALSE);
            con.createContainer(new BucketDirectory("home"));
            con.putBlob(new BucketPath("/home/file1.txt"), new SimplePayloadImpl(new SimpleStorageMetadataImpl(), "affe".getBytes()));
            Assert.assertTrue(new File(relativeDirAsAbsoluteDir).exists());

        } catch (Exception e) {
            throw BaseExceptionHandler.handle(e);
        }
    }


    @Test
    public void absolutePath() {
        try {
            String tmpdir = System.getProperty("java.io.tmpdir");
            LOGGER.debug("tempdir " + tmpdir);
            if (File.separatorChar == '/')     {
            	Assert.assertTrue(tmpdir.startsWith(BucketPath.BUCKET_SEPARATOR));
            } else {
            	// Prevents "Bucket must not contain uppercase letters: C:\Users\XXX\AppData\Local\Temp\ on Windows
            	tmpdir = tmpdir.toLowerCase();
            }
            
            if (!tmpdir.endsWith(BucketPath.BUCKET_SEPARATOR)) {
                tmpdir = tmpdir + BucketPath.BUCKET_SEPARATOR;
            }
            Assert.assertTrue(tmpdir.endsWith(BucketPath.BUCKET_SEPARATOR));
            String absoluteDir = tmpdir + "target" + BucketPath.BUCKET_SEPARATOR + UUID.randomUUID().toString();
            LOGGER.debug("my absolute path " + absoluteDir);
            Assert.assertFalse(new File(absoluteDir).exists());
            FileSystemExtendedStorageConnection con = new FileSystemExtendedStorageConnection(new FilesystemRootBucketName(absoluteDir), ConnectionProperties.defaultEncryptionPassword, BucketPathEncryptionFilenameOnly.FALSE);
            con.createContainer(new BucketDirectory("home"));
            con.putBlob(new BucketPath("/home/file1.txt"), new SimplePayloadImpl(new SimpleStorageMetadataImpl(), "affe".getBytes()));
            LOGGER.debug(absoluteDir);
            Assert.assertTrue(new File(absoluteDir).exists());


        } catch (Exception e) {
            throw BaseExceptionHandler.handle(e);
        }
    }
}
