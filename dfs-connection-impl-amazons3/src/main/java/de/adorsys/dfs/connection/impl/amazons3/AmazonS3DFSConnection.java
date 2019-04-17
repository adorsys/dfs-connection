package de.adorsys.dfs.connection.impl.amazons3;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import de.adorsys.common.exceptions.BaseException;
import de.adorsys.common.exceptions.BaseExceptionHandler;
import de.adorsys.common.utils.Frame;
import de.adorsys.dfs.connection.api.complextypes.BucketDirectory;
import de.adorsys.dfs.connection.api.complextypes.BucketPath;
import de.adorsys.dfs.connection.api.complextypes.BucketPathUtil;
import de.adorsys.dfs.connection.api.domain.Payload;
import de.adorsys.dfs.connection.api.domain.PayloadStream;
import de.adorsys.dfs.connection.api.exceptions.StorageConnectionException;
import de.adorsys.dfs.connection.api.service.api.DFSConnection;
import de.adorsys.dfs.connection.api.service.impl.SimplePayloadImpl;
import de.adorsys.dfs.connection.api.service.impl.SimplePayloadStreamImpl;
import de.adorsys.dfs.connection.api.types.ExtendedStoreConnectionType;
import de.adorsys.dfs.connection.api.types.ListRecursiveFlag;
import de.adorsys.dfs.connection.api.types.connection.AmazonS3AccessKey;
import de.adorsys.dfs.connection.api.types.connection.AmazonS3Region;
import de.adorsys.dfs.connection.api.types.connection.AmazonS3RootBucketName;
import de.adorsys.dfs.connection.api.types.connection.AmazonS3SecretKey;
import de.adorsys.dfs.connection.api.types.properties.AmazonS3ConnectionProperties;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by peter on 17.09.18.
 */
public class AmazonS3DFSConnection implements DFSConnection {
    private final static Logger LOGGER = LoggerFactory.getLogger(AmazonS3DFSConnection.class);
    private static final Logger SPECIAL_LOGGER = LoggerFactory.getLogger("SPECIAL_LOGGER");
    private AmazonS3 connection = null;
    private final static String AMAZONS3_TMP_FILE_PREFIX = "AMAZONS3_TMP_FILE_";
    private final static String AMAZONS3_TMP_FILE_SUFFIX = "";
    private static final String STORAGE_METADATA_KEY = "StorageMetadata";
    private final static int AMAZON_S3_META_LIMIT = 1024 * 2;
    private BucketDirectory amazonS3RootBucket;
    private BucketDirectory amazonS3RootContainersBucket;
    private AmazonS3Region amazonS3Region;

    public AmazonS3DFSConnection(AmazonS3ConnectionProperties properties) {
        this(properties.getUrl(), properties.getAmazonS3AccessKey(), properties.getAmazonS3SecretKey(), properties.getAmazonS3Region(), properties.getAmazonS3RootBucketName());
    }

    public AmazonS3DFSConnection(URL url,
                                 AmazonS3AccessKey accessKey,
                                 AmazonS3SecretKey secretKey,
                                 AmazonS3Region anAmazonS3Region,
                                 AmazonS3RootBucketName anAmazonS3RootBucketName) {
        amazonS3Region = anAmazonS3Region;
        amazonS3RootBucket = new BucketDirectory(anAmazonS3RootBucketName.getValue());
        amazonS3RootContainersBucket = new BucketDirectory(amazonS3RootBucket.getObjectHandle().getContainer() + ".containers");
        Frame frame = new Frame();
        frame.add("USE AMAZON S3 COMPLIANT SYSTEM");
        frame.add("(has be up and running)");
        frame.add("url: " + url.toString());
        frame.add("accessKey:   " + accessKey);
        frame.add("secretKey:   " + secretKey);
        frame.add("region:      " + amazonS3Region);
        frame.add("root bucket: " + amazonS3RootBucket);
        LOGGER.debug(frame.toString());

        AWSCredentialsProvider credentialsProvider = new AWSCredentialsProvider() {
            @Override
            public AWSCredentials getCredentials() {
                return new BasicAWSCredentials(accessKey.getValue(), secretKey.getValue());
            }

            @Override
            public void refresh() {

            }
        };

        AwsClientBuilder.EndpointConfiguration endpoint = new AwsClientBuilder.EndpointConfiguration(url.toString(), amazonS3Region.getValue());

        ClientConfiguration clientConfig = new ClientConfiguration();
        // clientConfig.setSocketTimeout(10000);
        clientConfig.setProtocol(Protocol.HTTP);
        clientConfig.disableSocketProxy();
        connection = AmazonS3ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withEndpointConfiguration(endpoint)
                .withClientConfiguration(clientConfig)
                .withPayloadSigningEnabled(false)
                .enablePathStyleAccess()
                .build();

        if (!connection.doesBucketExistV2(amazonS3RootBucket.getObjectHandle().getContainer())) {
            connection.createBucket(amazonS3RootBucket.getObjectHandle().getContainer());
        }
        if (!connection.doesBucketExistV2(amazonS3RootContainersBucket.getObjectHandle().getContainer())) {
            connection.createBucket(amazonS3RootContainersBucket.getObjectHandle().getContainer());
        }
    }

    @Override
    public void putBlob(BucketPath bucketPath, Payload payload) {
        LOGGER.debug("putBlob " + bucketPath);
        InputStream inputStream = new ByteArrayInputStream(payload.getData());
        PayloadStream payloadStream = new SimplePayloadStreamImpl(inputStream);
        putBlobStreamWithMemory(bucketPath, payloadStream, payload.getData().length);
    }

    @Override
    public Payload getBlob(BucketPath bucketPath) {
        try {
            PayloadStream payloadStream = getBlobStream(bucketPath);
            byte[] content = IOUtils.toByteArray(payloadStream.openStream());
            Payload payload = new SimplePayloadImpl(content);
            return payload;
        } catch (Exception e) {
            throw BaseExceptionHandler.handle(e);
        }
    }

    @Override
    public void putBlobStream(BucketPath bucketPath, PayloadStream payloadStream) {
        putBlobStreamWithTempFile(bucketPath, payloadStream);
    }

    @Override
    public PayloadStream getBlobStream(BucketPath abucketPath) {
        LOGGER.debug("getBlobStream " + abucketPath);
        BucketPath bucketPath = amazonS3RootBucket.append(abucketPath);

        GetObjectRequest getObjectRequest = new GetObjectRequest(bucketPath.getObjectHandle().getContainer(), bucketPath.getObjectHandle().getName());
        S3Object object = connection.getObject(getObjectRequest);
        S3ObjectInputStream objectContent = object.getObjectContent();
        PayloadStream payloadStream = new SimplePayloadStreamImpl(objectContent);
        LOGGER.debug("read ok for " + bucketPath);
        return payloadStream;
    }

    @Override
    public boolean blobExists(BucketPath abucketPath) {
        LOGGER.debug("blobExists " + abucketPath);
        BucketPath bucketPath = amazonS3RootBucket.append(abucketPath);

        // actually using exceptions is not nice, but it seems to be much faster than any list command
        try {
            connection.getObjectMetadata(bucketPath.getObjectHandle().getContainer(), bucketPath.getObjectHandle().getName());
            LOGGER.debug("blob exists " + abucketPath + " TRUE");
            return true;
        } catch (AmazonS3Exception e) {
            if (e.getMessage().contains("404 Not Found")) {
                LOGGER.debug("blob exists " + abucketPath + " FALSE");
                return false;
            }
            throw BaseExceptionHandler.handle(e);
        } catch (Exception e) {
            throw BaseExceptionHandler.handle(e);
        }
    }

    @Override
    public void removeBlob(BucketPath abucketPath) {
        LOGGER.debug("removeBlob " + abucketPath);
        BucketPath bucketPath = amazonS3RootBucket.append(abucketPath);

        connection.deleteObject(bucketPath.getObjectHandle().getContainer(), bucketPath.getObjectHandle().getName());
    }

    @Override
    public void removeBlobFolder(BucketDirectory bucketDirectory) {
        LOGGER.debug("removeBlobFolder " + bucketDirectory);
        if (bucketDirectory.getObjectHandle().getName() == null) {
            throw new StorageConnectionException("not a valid bucket directory " + bucketDirectory);
        }
        internalRemoveMultiple(bucketDirectory);
    }

    @Override
    public boolean containerExists(BucketDirectory bucketDirectory) {
        BucketPath bucketPath = amazonS3RootContainersBucket.appendName(bucketDirectory.getObjectHandle().getContainer());
        try {
            // Nicht schön hier mit Exceptions zu arbeiten, aber schneller als mit list
            connection.getObjectMetadata(bucketPath.getObjectHandle().getContainer(), bucketPath.getObjectHandle().getName());
            LOGGER.debug("containerExists " + bucketDirectory + " TRUE");
            return true;
        } catch (AmazonS3Exception e) {
            if (e.getMessage().contains("404 Not Found")) {
                LOGGER.debug("containerExists " + bucketDirectory + " FALSE (EXCEPTION)");
                return false;
            }
            throw BaseExceptionHandler.handle(e);
        } catch (Exception e) {
            throw BaseExceptionHandler.handle(e);
        }
    }

    @Override
    public void createContainer(BucketDirectory bucketDirectory) {
        LOGGER.debug("createContainer " + bucketDirectory);

        if (!containerExists(bucketDirectory)) {
            BucketPath bucketPath = amazonS3RootContainersBucket.appendName(bucketDirectory.getObjectHandle().getContainer());

            byte[] content = "x".getBytes();
            LOGGER.debug("write " + bucketPath);
            ObjectMetadata objectMetadata = new ObjectMetadata();
            objectMetadata.setContentLength(content.length);
            ByteArrayInputStream bis = new ByteArrayInputStream(content);

            PutObjectRequest putObjectRequest = new PutObjectRequest(bucketPath.getObjectHandle().getContainer(),
                    bucketPath.getObjectHandle().getName(),
                    bis, objectMetadata);
            PutObjectResult putObjectResult = connection.putObject(putObjectRequest);
            // LOGGER.debug("write of stream for :" + bucketPath + " -> " + putObjectResult.toString());
        }
    }


    @Override
    public void deleteContainer(BucketDirectory bucketDirectory) {
        LOGGER.debug("deleteContainer " + bucketDirectory);
        internalRemoveMultiple(new BucketDirectory(bucketDirectory.getObjectHandle().getContainer()));
    }

    @Override
    public List<BucketPath> list(BucketDirectory abucketDirectory, ListRecursiveFlag listRecursiveFlag) {
        LOGGER.debug("list " + abucketDirectory);
        List<BucketPath> returnList = new ArrayList<>();
        if (!containerExists(abucketDirectory)) {
            LOGGER.debug("return empty list for " + abucketDirectory);
            return returnList;
        }

        if (blobExists(new BucketPath(BucketPathUtil.getAsString(abucketDirectory)))) {
            // diese If-Abfrage dient dem Spezialfall, dass jemand einen BucketPath als BucketDirectory uebergeben hat.
            // Dann gibt es diesen bereits als file, dann muss eine leere Liste zurücgeben werden
            return new ArrayList<>();
        }

        BucketDirectory bucketDirectory = amazonS3RootBucket.append(abucketDirectory);

        String container = bucketDirectory.getObjectHandle().getContainer();
        String prefix = bucketDirectory.getObjectHandle().getName();
        if (prefix == null) {
            prefix = BucketPath.BUCKET_SEPARATOR;
        } else {
            prefix = BucketPath.BUCKET_SEPARATOR + prefix;
        }

        LOGGER.debug("search in " + container + " with prefix " + prefix + " " + listRecursiveFlag);
        String searchKey = prefix.substring(1); // remove first slash
        ObjectListing ol = connection.listObjects(container, searchKey);
        final List<String> keys = new ArrayList<>();
        ol.getObjectSummaries().forEach(el -> keys.add(BucketPath.BUCKET_SEPARATOR + el.getKey()));
        returnList = filter(container, prefix, keys, listRecursiveFlag);
        return returnList;
    }

    @Override
    public List<BucketDirectory> listAllBuckets() {
        LOGGER.debug("listAllBuckets");
        List<BucketDirectory> buckets = new ArrayList<>();
        ObjectListing ol = connection.listObjects(amazonS3RootContainersBucket.getObjectHandle().getContainer());
        ol.getObjectSummaries().forEach(bucket -> buckets.add(new BucketDirectory(bucket.getKey())));
        return buckets;
    }

    @Override
    public ExtendedStoreConnectionType getType() {
        return ExtendedStoreConnectionType.AMAZONS3;
    }

    public void cleanDatabase() {
        LOGGER.warn("DELETE DATABASE");
        for (BucketDirectory bucketDirectory : listAllBuckets()) {
            deleteContainer(bucketDirectory);
        }
    }

    public void showDatabase() {
        try {
            ObjectListing ol = connection.listObjects(amazonS3RootBucket.getObjectHandle().getContainer());
            List<DeleteObjectsRequest.KeyVersion> keys = new ArrayList<>();
            for (S3ObjectSummary key : ol.getObjectSummaries()) {
                LOGGER.debug(key.getKey());
            }
        } catch (Exception e) {
            throw BaseExceptionHandler.handle(e);
        }
    }


    // ==========================================================================

    List<BucketPath> filter(String container, String prefix, final List<String> keys, ListRecursiveFlag recursive) {
        List<BucketPath> result = new ArrayList<>();
        Set<String> dirs = new HashSet<>();

        // showKeys(keys);
        int numberOfDelimitersOfPrefix = StringUtils.countMatches(prefix, BucketPath.BUCKET_SEPARATOR);
        if (prefix.length() > BucketPath.BUCKET_SEPARATOR.length()) {
            numberOfDelimitersOfPrefix++;
        }
        int numberOfDelimitersExpected = numberOfDelimitersOfPrefix;

        keys.forEach(key -> {
            if (recursive.equals(ListRecursiveFlag.TRUE)) {
                result.add(new BucketPath(key));
            } else {
                int numberOfDelimitersOfKey = StringUtils.countMatches(key, BucketPath.BUCKET_SEPARATOR);
                if (numberOfDelimitersOfKey == numberOfDelimitersExpected) {
                    result.add(new BucketPath(key));
                }
            }

            if (recursive.equals(ListRecursiveFlag.TRUE)) {
                int fromIndex = prefix.length();
                while (fromIndex != -1) {
                    fromIndex = key.indexOf(BucketPath.BUCKET_SEPARATOR, fromIndex + 1);
                    if (fromIndex != -1) {
                        String dir = key.substring(0, fromIndex);
                        if (dir.length() == 0) {
                            dir = BucketPath.BUCKET_SEPARATOR;
                        }
                        dirs.add(dir);
                    }
                }
            } else {
                int fromIndex = prefix.length();
                int counter = 0;
                while (fromIndex != -1 && counter < 1) {
                    fromIndex = key.indexOf(BucketPath.BUCKET_SEPARATOR, fromIndex + 1);
                    if (fromIndex != -1) {
                        counter++;
                        String dir = key.substring(0, fromIndex);
                        if (dir.length() == 0) {
                            dir = BucketPath.BUCKET_SEPARATOR;
                        }
                        dirs.add(dir);
                    }
                }
            }

        });
        showResult(result);
        return result;
    }


    private void putBlobStreamWithMemory(BucketPath abucketPath, PayloadStream payloadStream, int size) {
        try {
            LOGGER.debug("putBlobStreamWithMemory " + abucketPath + " with known length " + size);
            ObjectMetadata objectMetadata = new ObjectMetadata();
            objectMetadata.setContentLength(size);

            BucketPath bucketPath = amazonS3RootBucket.append(abucketPath);

            PutObjectRequest putObjectRequest = new PutObjectRequest(
                    bucketPath.getObjectHandle().getContainer(),
                    bucketPath.getObjectHandle().getName(),
                    payloadStream.openStream(),
                    objectMetadata);
            PutObjectResult putObjectResult = connection.putObject(putObjectRequest);
            // LOGGER.debug("write of stream for :" + bucketPath + " -> " + putObjectResult.toString());
        } catch (Exception e) {
            throw BaseExceptionHandler.handle(e);
        }
    }

    private void putBlobStreamWithTempFile(BucketPath abucketPath, PayloadStream payloadStream) {
        try {
            LOGGER.debug("putBlobStreamWithTempFile " + abucketPath + " to tmpfile with unknown size");
            File targetFile = File.createTempFile(AMAZONS3_TMP_FILE_PREFIX, AMAZONS3_TMP_FILE_SUFFIX);
            try (InputStream is = payloadStream.openStream()) {
                Files.copy(
                        is,
                        targetFile.toPath(),
                        StandardCopyOption.REPLACE_EXISTING);
            }

            LOGGER.debug(abucketPath + " with tmpfile " + targetFile.getAbsolutePath() + " written with " + targetFile.length() + " bytes -> will now be copied to ceph");
            try (FileInputStream fis = new FileInputStream(targetFile)) {
                ObjectMetadata objectMetadata = new ObjectMetadata();
                objectMetadata.setContentLength(targetFile.length());

                BucketPath bucketPath = amazonS3RootBucket.append(abucketPath);

                PutObjectRequest putObjectRequest = new PutObjectRequest(bucketPath.getObjectHandle().getContainer(), bucketPath.getObjectHandle().getName(), fis, objectMetadata);
                PutObjectResult putObjectResult = connection.putObject(putObjectRequest);
                LOGGER.debug("stored " + bucketPath + " to ceph with size " + targetFile.length());
            }

            targetFile.delete();
        } catch (Exception e) {
            throw BaseExceptionHandler.handle(e);
        }
    }

    /**
     * Löscht alles unterhalb und einschließlich des genannten bucketDirectories
     *
     * @param abucketDirectory
     */
    private void internalRemoveMultiple(BucketDirectory abucketDirectory) {
        LOGGER.debug("internalRemoveMultiple " + abucketDirectory);
        if (!containerExists(abucketDirectory)) {
            return;
        }

        BucketDirectory bucketDirectory = amazonS3RootBucket.append(abucketDirectory);

        String container = bucketDirectory.getObjectHandle().getContainer();
        String prefix = bucketDirectory.getObjectHandle().getName();
        if (prefix == null) {
            prefix = "";
        }
        LOGGER.debug("listObjects(" + container + "," + prefix + ")");
        ObjectListing ol = connection.listObjects(container, prefix);
        if (ol.getObjectSummaries().isEmpty()) {
            LOGGER.debug("no files found in " + container + " with prefix " + prefix);
        }

        List<DeleteObjectsRequest.KeyVersion> keys = new ArrayList<>();
        for (S3ObjectSummary key : ol.getObjectSummaries()) {
            keys.add(new DeleteObjectsRequest.KeyVersion(key.getKey()));
            if (keys.size() == 100) {
                DeleteObjectsRequest deleteObjectsRequest = new DeleteObjectsRequest(container);
                deleteObjectsRequest.setKeys(keys);
                LOGGER.debug("DELETE CHUNK CONTENTS OF BUCKET " + container + " with " + keys.size() + " elements");
                DeleteObjectsResult deleteObjectsResult = connection.deleteObjects(deleteObjectsRequest);
                LOGGER.debug("SERVER CONFIRMED DELETION OF " + deleteObjectsResult.getDeletedObjects().size() + " elements");
                ObjectListing ol2 = connection.listObjects(container);
                LOGGER.debug("SERVER has remaining " + ol2.getObjectSummaries().size() + " elements");
                if (ol2.getObjectSummaries().size() == ol.getObjectSummaries().size()) {
                    throw new BaseException("Fatal error. Server confirmied deleltion of " + keys.size() + " elements, but still " + ol.getObjectSummaries().size() + " elementents in " + container);
                }
            }
        }
        if (!keys.isEmpty()) {
            DeleteObjectsRequest deleteObjectsRequest = new DeleteObjectsRequest(container);
            deleteObjectsRequest.setKeys(keys);
            LOGGER.debug("DELETE CONTENTS OF BUCKET " + container + " with " + keys.size() + " elements");
            DeleteObjectsResult deleteObjectsResult = connection.deleteObjects(deleteObjectsRequest);
            LOGGER.debug("SERVER CONFIRMED DELETION OF " + deleteObjectsResult.getDeletedObjects().size() + " elements");
        }
        if (abucketDirectory.getObjectHandle().getName() == null) {
            BucketPath containerFile = amazonS3RootContainersBucket.appendName(abucketDirectory.getObjectHandle().getContainer());
            connection.deleteObject(containerFile.getObjectHandle().getContainer(), containerFile.getObjectHandle().getName());
        }
    }

    private void showResult(List<BucketPath> result) {
        LOGGER.debug("nachher:");
        result.forEach(el -> {
            LOGGER.debug(el.toString());
        });
    }

    private void showKeys(List<String> keys) {
        LOGGER.debug("vorher");
        keys.forEach(el -> {
            LOGGER.debug(el);
        });
    }

}
