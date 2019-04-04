package de.adorsys.dfs.connection.api.filesystem;

import de.adorsys.common.exceptions.BaseExceptionHandler;
import de.adorsys.dfs.connection.api.complextypes.BucketDirectory;
import de.adorsys.dfs.connection.api.complextypes.BucketPath;
import de.adorsys.dfs.connection.api.domain.Payload;
import de.adorsys.dfs.connection.api.domain.PayloadStream;
import de.adorsys.dfs.connection.api.exceptions.StorageConnectionException;
import de.adorsys.dfs.connection.api.service.impl.SimplePayloadImpl;
import de.adorsys.dfs.connection.api.service.impl.SimplePayloadStreamImpl;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.UUID;

/**
 * Created by peter on 21.02.18 at 19:31.
 */
class FileHelper {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileHelper.class);
    private boolean absolutePath = false;


    protected BucketDirectory baseDir;

    public FileHelper(BucketDirectory bucketDirectory, boolean absolutePath) {
        this.baseDir = bucketDirectory;
        this.absolutePath = absolutePath;
    }


    public void writePayload(BucketPath bucketPath, SimplePayloadImpl payload) {

        try {
            byte[] content = payload.getData();

            createDirectoryIfNecessary(bucketPath);
            File tempFile = BucketPathFileHelper.getAsFile(baseDir.append(bucketPath.add("." + UUID.randomUUID().toString())), absolutePath);
            if (tempFile.exists()) {
                throw new StorageConnectionException("Temporary File exists. This must not happen." + tempFile);
            }
            LOGGER.debug("write temporary file to " + tempFile);

            try (OutputStream zos = new BufferedOutputStream(new FileOutputStream(tempFile))) {
                zos.write(content, 0, content.length);
                zos.close();
            }

            File origFile = BucketPathFileHelper.getAsFile(baseDir.append(bucketPath), absolutePath);
            /*
            if (origFile.exists()) {
                LOGGER.debug("ACHTUNG, file existiert bereits, wird nun neu verlinkt " + bucketPath);
                FileUtils.forceDelete(origFile);
            }
            FileUtils.moveFile(tempFile, origFile);
            */
            // This should work much faster as the above code, because there is no need to delete the file
            Files.move(tempFile.toPath(), origFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        } catch (Exception e) {
            throw BaseExceptionHandler.handle(e);
        }
    }

    public void writePayloadStream(BucketPath bucketPath, SimplePayloadStreamImpl payloadStream) {

        try {
            createDirectoryIfNecessary(bucketPath);
            File tempFile = BucketPathFileHelper.getAsFile(baseDir.append(bucketPath.add("." + UUID.randomUUID().toString())), absolutePath);
            if (tempFile.exists()) {
                throw new StorageConnectionException("Temporary File exists. This must not happen." + tempFile);
            }
            LOGGER.debug("write temporary file to " + tempFile);

            try (OutputStream zos = new BufferedOutputStream(new FileOutputStream(tempFile))) {
                try (InputStream is = payloadStream.openStream()) {
                    IOUtils.copy(is, zos);
                }
            }

            File origFile = BucketPathFileHelper.getAsFile(baseDir.append(bucketPath), absolutePath);
            if (origFile.exists()) {
                LOGGER.debug("ACHTUNG, file existiert bereits, wird nun neu verlinkt " + bucketPath);
                FileUtils.forceDelete(origFile);
            }
            FileUtils.moveFile(tempFile, origFile);
        } catch (Exception e) {
            throw BaseExceptionHandler.handle(e);
        }
    }


    public Payload readPayload(BucketPath bucketPath) {
        try {
            File file = BucketPathFileHelper.getAsFile(baseDir.append(bucketPath), absolutePath);
            try (InputStream zis = new BufferedInputStream(new FileInputStream(file))) {
                byte[] data = IOUtils.toByteArray(zis);
                zis.close();
                Payload payload = new SimplePayloadImpl(data);
                return payload;
            }
        } catch (Exception e) {
            throw BaseExceptionHandler.handle(e);
        }
    }

    public PayloadStream readPayloadStream(BucketPath bucketPath) {
        try {
            File file = BucketPathFileHelper.getAsFile(baseDir.append(bucketPath), absolutePath);
            InputStream zis = new BufferedInputStream(new FileInputStream(file));

            return new SimplePayloadStreamImpl(zis);

        } catch (Exception e) {
            throw BaseExceptionHandler.handle(e);
        }
    }

    private void createDirectoryIfNecessary(BucketPath bucketPath) {
        File dir = BucketPathFileHelper.getAsFile(baseDir.append(bucketPath).getBucketDirectory(), absolutePath);
        if (dir.exists()) {
            return;
        }
        boolean success = dir.mkdirs();
        if (!success) {
            throw new StorageConnectionException("cant create directory " + dir);
        }

    }


}
