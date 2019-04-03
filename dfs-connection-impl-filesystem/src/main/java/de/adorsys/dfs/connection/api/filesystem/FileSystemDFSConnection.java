package de.adorsys.dfs.connection.api.filesystem;

import de.adorsys.common.exceptions.BaseException;
import de.adorsys.common.exceptions.BaseExceptionHandler;
import de.adorsys.common.utils.Frame;
import de.adorsys.dfs.connection.api.complextypes.BucketDirectory;
import de.adorsys.dfs.connection.api.complextypes.BucketPath;
import de.adorsys.dfs.connection.api.complextypes.BucketPathUtil;
import de.adorsys.dfs.connection.api.domain.Payload;
import de.adorsys.dfs.connection.api.domain.PayloadStream;
import de.adorsys.dfs.connection.api.exceptions.StorageConnectionException;
import de.adorsys.dfs.connection.api.filesystem.exceptions.*;
import de.adorsys.dfs.connection.api.service.api.DFSConnection;
import de.adorsys.dfs.connection.api.service.impl.SimplePayloadImpl;
import de.adorsys.dfs.connection.api.service.impl.SimplePayloadStreamImpl;
import de.adorsys.dfs.connection.api.types.ExtendedStoreConnectionType;
import de.adorsys.dfs.connection.api.types.ListRecursiveFlag;
import de.adorsys.dfs.connection.api.types.connection.FilesystemRootBucketName;
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
    private final static Logger LOGGER = LoggerFactory.getLogger(FileSystemDFSConnection.class);
    protected final BucketDirectory baseDir;
    private ZipFileHelper zipFileHelper;
    private boolean absolutePath = false;

    public FileSystemDFSConnection(FilesystemConnectionProperties properties) {
        this(properties.getFilesystemRootBucketName());
    }
    public FileSystemDFSConnection(FilesystemRootBucketName basedir) {
        try {
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
            LOGGER.info(frame.toString());

            this.zipFileHelper = new ZipFileHelper(this.baseDir, absolutePath);
        } catch (Exception e) {
            throw BaseExceptionHandler.handle(e);
        }
    }


    @Override
    public void createContainer(BucketDirectory bucketDirectory) {
        LOGGER.debug("createContainer " + bucketDirectory);
        String containerOnly = bucketDirectory.getObjectHandle().getContainer();

        File file = BucketPathFileHelper.getAsFile(baseDir.appendDirectory(containerOnly), absolutePath);
        if (file.isDirectory()) {
            LOGGER.debug("directory already exists:" + file);
            return;
        }
        boolean success = file.mkdirs();
        if (!success) {
            throw new CreateFolderException("Can not create directory " + file);
        }
        LOGGER.debug("created folder " + file);
    }

    @Override
    public boolean containerExists(BucketDirectory bucketDirectory) {
        File file = BucketPathFileHelper.getAsFile(baseDir.appendDirectory(bucketDirectory.getObjectHandle().getContainer()), absolutePath);
        if (file.isDirectory()) {
            LOGGER.debug("directory exists:" + file);
            return true;
        }
        if (file.isFile()) {
            throw new FolderIsAFileException("folder is a file " + file);
        }
        LOGGER.debug("directory does not exists" + file);
        return false;
    }


    @Override
    public void deleteContainer(BucketDirectory container) {
        LOGGER.debug("deleteContainer " + container);
        File file = BucketPathFileHelper.getAsFile(baseDir.appendDirectory(container.getObjectHandle().getContainer()), absolutePath);
        if (!containerExists(container)) {
            LOGGER.debug("directory does not exist. so nothing to delete:" + file);
            return;
        }
        try {
            FileUtils.deleteDirectory(file);
        } catch (IOException e) {
            throw new FolderDeleteException("can not delete " + file, e);
        }
    }

    @Override
    public boolean blobExists(BucketPath bucketPath) {
        LOGGER.debug("blobExists " + bucketPath);
        File file = BucketPathFileHelper.getAsFile(baseDir.append(bucketPath.add(ZipFileHelper.ZIP_SUFFIX)), absolutePath);
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
        String[] extensions = new String[1];
        extensions[0] = ZipFileHelper.ZIP_SUFFIX.substring(1);
        Collection<File> files = FileUtils.listFiles(base, extensions, listRecursiveFlag.equals(ListRecursiveFlag.TRUE));
        int lengthToSkip = BucketPathFileHelper.getAsFile(baseDir, absolutePath).getPath().length();
        int extToSkip = ZipFileHelper.ZIP_SUFFIX.length();
        for(File file: files) {
            String filenameWithExtension = file.getPath().substring(lengthToSkip);
            String filenameWithoutExtension = filenameWithExtension.substring(0,filenameWithExtension.length()-extToSkip);

            result.add(new BucketPath(filenameWithoutExtension));
        }
        return result;
    }

    @Override
    public List<BucketDirectory> listAllBuckets() {
        LOGGER.debug("listAllbuckeets");
        try {
            List<BucketDirectory> list = new ArrayList<>();
            String[] dirs = BucketPathFileHelper.getAsFile(baseDir, absolutePath).list(new DirectoryFilenameFilter());
            if (dirs == null) {
                return list;
            }
            Arrays.stream(dirs).forEach(dir -> list.add(new BucketDirectory(dir)));
            return list;
        } catch (Exception e) {
            throw BaseExceptionHandler.handle(e);
        }
    }

    @Override
    public ExtendedStoreConnectionType getType() {
        return ExtendedStoreConnectionType.FILESYSTEM;
    }

    @Override
    public void putBlob(BucketPath bucketPath, Payload payload) {
        LOGGER.debug("putBlob " + bucketPath);
        checkContainerExists(bucketPath);
        zipFileHelper.writeZip(bucketPath, new SimplePayloadImpl(payload));
    }

    @Override
    public Payload getBlob(BucketPath bucketPath) {
        LOGGER.debug("getBlob " + bucketPath);
        checkContainerExists(bucketPath);
        return zipFileHelper.readZip(bucketPath);
    }

    @Override
    public void putBlobStream(BucketPath bucketPath, PayloadStream payloadStream) {
        LOGGER.debug("putBlobStream " + bucketPath);
        checkContainerExists(bucketPath);
        zipFileHelper.writeZipStream(bucketPath, new SimplePayloadStreamImpl(payloadStream));

    }

    @Override
    public PayloadStream getBlobStream(BucketPath bucketPath) {
        LOGGER.debug("getBlobStrea " + bucketPath);
        checkContainerExists(bucketPath);
        return zipFileHelper.readZipStream(bucketPath);
    }

    @Override
    public void removeBlob(BucketPath bucketPath) {
        LOGGER.debug("removeBlob " + bucketPath);
        checkContainerExists(bucketPath);
        File file = BucketPathFileHelper.getAsFile(baseDir.append(bucketPath).add(ZipFileHelper.ZIP_SUFFIX), absolutePath);
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
        checkContainerExists(bucketDirectory);
        if (bucketDirectory.getObjectHandle().getName() == null) {
            throw new StorageConnectionException("not a valid bucket directory " + bucketDirectory);
        }
        File directory = BucketPathFileHelper.getAsFile(baseDir.append(bucketDirectory), absolutePath);
        LOGGER.debug("remove directory " + directory.getAbsolutePath());
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

    private void checkContainerExists(BucketPath bucketPath) {
        if (!containerExists(bucketPath.getBucketDirectory())) {
            throw new BaseException("Container " + bucketPath.getObjectHandle().getContainer() + " does not exist");
        }
    }

    private void checkContainerExists(BucketDirectory bucketDirectory) {
        if (!containerExists(bucketDirectory)) {
            throw new BaseException("Container " + bucketDirectory.getObjectHandle().getContainer() + " does not exist");
        }
    }

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
