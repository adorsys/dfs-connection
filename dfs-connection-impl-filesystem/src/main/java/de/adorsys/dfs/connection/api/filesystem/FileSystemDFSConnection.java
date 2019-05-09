package de.adorsys.dfs.connection.api.filesystem;

import de.adorsys.common.exceptions.BaseException;
import de.adorsys.common.exceptions.BaseExceptionHandler;
import de.adorsys.common.utils.Frame;
import de.adorsys.dfs.connection.api.complextypes.BucketDirectory;
import de.adorsys.dfs.connection.api.complextypes.BucketPath;
import de.adorsys.dfs.connection.api.domain.Payload;
import de.adorsys.dfs.connection.api.domain.PayloadStream;
import de.adorsys.dfs.connection.api.filesystem.exceptions.DeleteFileException;
import de.adorsys.dfs.connection.api.filesystem.exceptions.FileIsFolderException;
import de.adorsys.dfs.connection.api.service.api.DFSConnection;
import de.adorsys.dfs.connection.api.service.impl.SimplePayloadImpl;
import de.adorsys.dfs.connection.api.service.impl.SimplePayloadStreamImpl;
import de.adorsys.dfs.connection.api.types.ListRecursiveFlag;
import de.adorsys.dfs.connection.api.types.connection.FilesystemRootBucketName;
import de.adorsys.dfs.connection.api.types.properties.ConnectionProperties;
import de.adorsys.dfs.connection.api.types.properties.FilesystemConnectionProperties;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Created by peter on 06.02.18 at 12:40.
 */
public class FileSystemDFSConnection implements DFSConnection {
    private FilesystemConnectionPropertiesImpl connectionProperties;
    private final static Logger LOGGER = LoggerFactory.getLogger(FileSystemDFSConnection.class);
    protected final BucketDirectory baseDir;
    private FileHelper fileHelper;
    private boolean absolutePath = false;

    public FileSystemDFSConnection(FilesystemConnectionProperties properties) {
        this(properties.getFilesystemRootBucketName());
    }
    public FileSystemDFSConnection(FilesystemRootBucketName basedir) {
        try {
            connectionProperties = new FilesystemConnectionPropertiesImpl();
            connectionProperties.setFilesystemRootBucketName(basedir);

            this.baseDir = new BucketDirectory(basedir.getValue());
            this.absolutePath = (basedir.getValue().startsWith(BucketPath.BUCKET_SEPARATOR));
            Frame frame = new Frame();
            frame.add("USE FILE SYSTEM");
            if (!absolutePath) {
                String currentDir = new File(".").getCanonicalPath();
                String absoluteDirectory = basedir.getValue();
                absoluteDirectory = currentDir + BucketPath.BUCKET_SEPARATOR + absoluteDirectory;
                frame.add("basedir     : " + basedir);
                frame.add("absolutedir : " + absoluteDirectory);
            } else {
                frame.add("absolutedir : " + basedir);
            }
            LOGGER.debug(frame.toString());

            this.fileHelper = new FileHelper(this.baseDir, absolutePath);
        } catch (Exception e) {
            throw BaseExceptionHandler.handle(e);
        }
    }

    @Override
    public boolean blobExists(BucketPath bucketPath) {
        LOGGER.debug("blobExists " + bucketPath);
        File file = BucketPathFileHelper.getAsFile(baseDir.append(bucketPath), absolutePath);
        if (file.isDirectory()) {
            throw new FileIsFolderException("file " + file);
        }
        if (file.isFile()) {
            LOGGER.debug("file does exist " + file);
            return true;
        }
        LOGGER.debug("file does not exist " + file);
        return false;
    }

    @Override
    public List<BucketPath> list(BucketDirectory bucketDirectory, ListRecursiveFlag listRecursiveFlag) {
        LOGGER.debug("list " + bucketDirectory);
        List<BucketPath> result = new ArrayList<>();
        File base = BucketPathFileHelper.getAsFile(baseDir.append(bucketDirectory), absolutePath);
        if (!base.isDirectory()) {
            return result;
        }
        Collection<File> files = FileUtils.listFiles(base, null, listRecursiveFlag.equals(ListRecursiveFlag.TRUE));
        int lengthToSkip = BucketPathFileHelper.getAsFile(baseDir, absolutePath).getPath().length();
        for(File file: files) {
            String filenameWithExtension = file.getPath().substring(lengthToSkip);
            result.add(new BucketPath(filenameWithExtension));
        }
        return result;
    }

    @Override
    public void deleteDatabase() {
        removeBlobFolder(new BucketDirectory("/"));
    }

    @Override
    public ConnectionProperties getConnectionProperties() {
        return connectionProperties;
    }

    @Override
    public void putBlob(BucketPath bucketPath, Payload payload) {
        LOGGER.debug("putBlob " + bucketPath);
        fileHelper.writePayload(bucketPath, new SimplePayloadImpl(payload));
    }

    @Override
    public Payload getBlob(BucketPath bucketPath) {
        LOGGER.debug("getBlob " + bucketPath);
        return fileHelper.readPayload(bucketPath);
    }

    @Override
    public void putBlobStream(BucketPath bucketPath, PayloadStream payloadStream) {
        LOGGER.debug("putBlobStream " + bucketPath);
        fileHelper.writePayloadStream(bucketPath, new SimplePayloadStreamImpl(payloadStream));

    }

    @Override
    public PayloadStream getBlobStream(BucketPath bucketPath) {
        LOGGER.debug("getBlobStrea " + bucketPath);
        return fileHelper.readPayloadStream(bucketPath);
    }

    @Override
    public void removeBlob(BucketPath bucketPath) {
        LOGGER.debug("removeBlob " + bucketPath);
        File file = BucketPathFileHelper.getAsFile(baseDir.append(bucketPath), absolutePath);
        if (!file.exists()) {
            return;
        }
        try {
            FileUtils.forceDelete(file);
        } catch (IOException e) {
            throw new DeleteFileException("can not delete " + file, e);
        }
    }

    @Override
    public void removeBlobFolder(BucketDirectory bucketDirectory) {
        LOGGER.debug("removeBlobFolder " + bucketDirectory);
        File directory = BucketPathFileHelper.getAsFile(baseDir.append(bucketDirectory), absolutePath);
        LOGGER.debug("remove directory " + directory.getAbsolutePath());
        if (directory.getAbsolutePath().length() < 3) {
            throw new BaseException("just to make sure, root will never every be to be deleted");
        }
        if (!directory.exists()) {
            return;
        }
        try {
            FileUtils.forceDelete(directory);
        } catch (IOException e) {
            throw new DeleteFileException("can not delete " + directory, e);
        }

    }

    /* ===========================================================================================================
     */

    private final static class DirectoryFilenameFilter implements FilenameFilter {
        @Override
        public boolean accept(File dir, String name) {
            try {
                return new File(dir.getCanonicalPath() + BucketPath.BUCKET_SEPARATOR + name).isDirectory();
            } catch (IOException e) {
                throw BaseExceptionHandler.handle(e);
            }
        }
    }

}
