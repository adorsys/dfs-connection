package de.adorsys.dfs.connection.api.filesystem;

import de.adorsys.common.exceptions.BaseExceptionHandler;
import de.adorsys.dfs.connection.api.complextypes.BucketDirectory;
import de.adorsys.dfs.connection.api.complextypes.BucketPath;
import de.adorsys.dfs.connection.api.complextypes.BucketPathUtil;
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
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

/**
 * Created by peter on 21.02.18 at 19:31.
 */
public class ZipFileHelper {
    private static final Logger LOGGER = LoggerFactory.getLogger(ZipFileHelper.class);
    protected static final String ZIP_CONTENT_BINARY = "Content.binary";
    protected static final String ZIP_SUFFIX = ".zip";
    private boolean absolutePath = false;


    protected BucketDirectory baseDir;

    public ZipFileHelper(BucketDirectory bucketDirectory, boolean absolutePath) {
        this.baseDir = bucketDirectory;
        this.absolutePath = absolutePath;
    }

    /**
     * https://stackoverflow.com/questions/14462371/preferred-way-to-use-java-zipoutputstream-and-bufferedoutputstream
     */
    public void writeZip(BucketPath bucketPath, SimplePayloadImpl payload) {

        try {
            byte[] content = payload.getData();

            createDirectoryIfNecessary(bucketPath);
            File tempFile = BucketPathFileHelper.getAsFile(baseDir.append(bucketPath.add(ZIP_SUFFIX).add("." + UUID.randomUUID().toString())), absolutePath);
            if (tempFile.exists()) {
                throw new StorageConnectionException("Temporary File exists. This must not happen." + tempFile);
            }
            LOGGER.debug("write temporary zip file to " + tempFile);

            try (ZipOutputStream zos = new ZipOutputStream(new BufferedOutputStream(new FileOutputStream(tempFile)))) {

                zos.putNextEntry(new ZipEntry(ZIP_CONTENT_BINARY));
                zos.write(content, 0, content.length);
                zos.closeEntry();
            }

            File origFile = BucketPathFileHelper.getAsFile(baseDir.append(bucketPath.add(ZIP_SUFFIX)), absolutePath);
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

    public void writeZipStream(BucketPath bucketPath, SimplePayloadStreamImpl payloadStream) {

        try {
            createDirectoryIfNecessary(bucketPath);
            File tempFile = BucketPathFileHelper.getAsFile(baseDir.append(bucketPath.add(ZIP_SUFFIX).add("." + UUID.randomUUID().toString())), absolutePath);
            if (tempFile.exists()) {
                throw new StorageConnectionException("Temporary File exists. This must not happen." + tempFile);
            }
            LOGGER.debug("write temporary zip file to " + tempFile);

            try (ZipOutputStream zos = new ZipOutputStream(new BufferedOutputStream(new FileOutputStream(tempFile)))) {
                try (InputStream is = payloadStream.openStream()) {
                    zos.putNextEntry(new ZipEntry(ZIP_CONTENT_BINARY));
                    IOUtils.copy(is, zos);
                }
            }

            File origFile = BucketPathFileHelper.getAsFile(baseDir.append(bucketPath.add(ZIP_SUFFIX)), absolutePath);
            if (origFile.exists()) {
                LOGGER.debug("ACHTUNG, file existiert bereits, wird nun neu verlinkt " + bucketPath);
                FileUtils.forceDelete(origFile);
            }
            FileUtils.moveFile(tempFile, origFile);
        } catch (Exception e) {
            throw BaseExceptionHandler.handle(e);
        }
    }


    public Payload readZip(BucketPath bucketPath) {
        try {
            File file = BucketPathFileHelper.getAsFile(baseDir.append(bucketPath.add(ZIP_SUFFIX)), absolutePath);
            try (ZipInputStream zis = new ZipInputStream(new BufferedInputStream(new FileInputStream(file)))) {
                ZipEntry entry;
                byte[] data = null;
                while ((entry = zis.getNextEntry()) != null) {
                    if (entry.getName().equals(ZIP_CONTENT_BINARY)) {
                        data = IOUtils.toByteArray(zis);
                    }
                    zis.closeEntry();
                }
                if (data == null) {
                    throw new StorageConnectionException("Zipfile " + bucketPath + " does not have entry for " + ZIP_CONTENT_BINARY);
                }
                Payload payload = new SimplePayloadImpl(data);
                return payload;
            }
        } catch (Exception e) {
            throw BaseExceptionHandler.handle(e);
        }
    }

    public PayloadStream readZipStream(BucketPath bucketPath) {
        try {
            File file = BucketPathFileHelper.getAsFile(baseDir.append(bucketPath.add(ZIP_SUFFIX)), absolutePath);
            ZipInputStream zis = new ZipInputStream(new BufferedInputStream(new FileInputStream(file)));

            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                if (entry.getName().equals(ZIP_CONTENT_BINARY)) {
                    return new SimplePayloadStreamImpl(zis);
                }
                zis.closeEntry();
            }
            throw new StorageConnectionException("Zipfile " + bucketPath + " does not have entry for " + ZIP_CONTENT_BINARY);

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
