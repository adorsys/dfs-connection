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
import de.adorsys.dfs.connection.api.domain.Payload;
import de.adorsys.dfs.connection.api.domain.PayloadStream;
import de.adorsys.dfs.connection.api.service.api.DFSConnection;
import de.adorsys.dfs.connection.api.service.impl.SimplePayloadImpl;
import de.adorsys.dfs.connection.api.service.impl.SimplePayloadStreamImpl;
import de.adorsys.dfs.connection.api.types.ListRecursiveFlag;
import de.adorsys.dfs.connection.api.types.connection.AmazonS3AccessKey;
import de.adorsys.dfs.connection.api.types.connection.AmazonS3Region;
import de.adorsys.dfs.connection.api.types.connection.AmazonS3RootBucketName;
import de.adorsys.dfs.connection.api.types.connection.AmazonS3SecretKey;
import de.adorsys.dfs.connection.api.types.properties.AmazonS3ConnectionProperties;
import de.adorsys.dfs.connection.api.types.properties.ConnectionProperties;
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
import java.util.List;

/**
 * Created by peter on 17.09.18.
 */
public class AmazonS3DFSConnection implements DFSConnection {
    private AmazonS3ConnectionProperitesImpl connectionProperties;
    private final static Logger LOGGER = LoggerFactory.getLogger(AmazonS3DFSConnection.class);
    private AmazonS3 connection = null;
    private final static String AMAZONS3_TMP_FILE_PREFIX = "AMAZONS3_TMP_FILE_";
    private final static String AMAZONS3_TMP_FILE_SUFFIX = "";
    private BucketDirectory amazonS3RootBucket;
    private AmazonS3Region amazonS3Region;

    public AmazonS3DFSConnection(AmazonS3ConnectionProperties properties) {
        this(properties.getUrl(), properties.getAmazonS3AccessKey(), properties.getAmazonS3SecretKey(), properties.getAmazonS3Region(), properties.getAmazonS3RootBucketName());
    }

    public AmazonS3DFSConnection(URL url,
                                 AmazonS3AccessKey accessKey,
                                 AmazonS3SecretKey secretKey,
                                 AmazonS3Region anAmazonS3Region,
                                 AmazonS3RootBucketName anAmazonS3RootBucketName) {
        connectionProperties = new AmazonS3ConnectionProperitesImpl();
        connectionProperties.setUrl(url);
        connectionProperties.setAmazonS3AccessKey(accessKey);
        connectionProperties.setAmazonS3SecretKey(secretKey);
        connectionProperties.setAmazonS3Region(anAmazonS3Region);
        connectionProperties.setAmazonS3RootBucketName(anAmazonS3RootBucketName);

        amazonS3Region = anAmazonS3Region;
        amazonS3RootBucket = new BucketDirectory(anAmazonS3RootBucketName.getValue());
        Frame frame = new Frame();
        frame.add("USE AMAZON S3 COMPLIANT SYSTEM");
        frame.add("(has be up and running)");
        frame.add("url:              " + url.toString());
        frame.add("accessKey:        " + accessKey);
        frame.add("secretKey:        " + secretKey);
        frame.add("region:           " + amazonS3Region);
        frame.add("root bucket:      " + amazonS3RootBucket);
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

        if (!connection.doesBucketExistV2(amazonS3RootBucket.getContainer())) {
            connection.createBucket(amazonS3RootBucket.getContainer());
        }
    }

    @Override
    public ConnectionProperties getConnectionProperties() {
        return connectionProperties;
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

        GetObjectRequest getObjectRequest = new GetObjectRequest(bucketPath.getContainer(), bucketPath.getName());
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
            connection.getObjectMetadata(bucketPath.getContainer(), bucketPath.getName());
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

        connection.deleteObject(bucketPath.getContainer(), bucketPath.getName());
    }

    @Override
    public void removeBlobFolder(BucketDirectory bucketDirectory) {
        LOGGER.debug("removeBlobFolder " + bucketDirectory);
        internalRemoveMultiple(bucketDirectory);
    }

    @Override
    public List<BucketPath> list(BucketDirectory abucketDirectory, ListRecursiveFlag listRecursiveFlag) {
        LOGGER.debug("list " + abucketDirectory);
        List<BucketPath> returnList = new ArrayList<>();

        if (!abucketDirectory.isRoot()) {
            if (blobExists(new BucketPath(abucketDirectory.getValue()))) {
                // diese If-Abfrage dient dem Spezialfall, dass jemand einen BucketPath als BucketDirectory uebergeben hat.
                // Dann gibt es diesen bereits als file, dann muss eine leere Liste zurücgeben werden
                return returnList;
            }
        }

        BucketDirectory bucketDirectory = amazonS3RootBucket.append(abucketDirectory);

        String container = bucketDirectory.getContainer();
        String prefix = bucketDirectory.getName();
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
        returnList = filter(amazonS3RootBucket, abucketDirectory, prefix, keys, listRecursiveFlag);
        return returnList;
    }

    @Override
    public void deleteDatabase() {
        removeBlobFolder(new BucketDirectory(BucketPath.BUCKET_SEPARATOR));
    }

    public void showDatabase() {
        try {
            ObjectListing ol = connection.listObjects(amazonS3RootBucket.getContainer());
            List<DeleteObjectsRequest.KeyVersion> keys = new ArrayList<>();
            for (S3ObjectSummary key : ol.getObjectSummaries()) {
                LOGGER.debug(key.getKey());
            }
        } catch (Exception e) {
            throw BaseExceptionHandler.handle(e);
        }
    }

//    public void deleteAllBuckets() {
//        try {
//            List<Bucket> buckets = connection.listBuckets();
//            for (Bucket bucket : buckets) {
//                LOGGER.info(bucket.getName());
//
//                ObjectListing objectListing = connection.listObjects(bucket.getName());
//                LOGGER.info("delete " + objectListing.getObjectSummaries().size() + " files from " + bucket.getName());
//                objectListing.getObjectSummaries().forEach(sum -> {
//                    connection.deleteObject(sum.getBucketName(), sum.getKey());
//                });
//                connection.deleteBucket(bucket.getName());
//            }
//        } catch (Exception e) {
//            throw BaseExceptionHandler.handle(e);
//        }
//    }


    // ==========================================================================

    List<BucketPath> filter(BucketDirectory rootBucketName, BucketDirectory searchDirectory, String prefix, final List<String> keys, ListRecursiveFlag recursive) {
        String rootDirectoryString = rootBucketName.getValue();
        String searchDirectoryString = searchDirectory.getValue();
        LOGGER.debug("recursive is " + recursive);
        LOGGER.debug("prefix    is " + prefix);
        LOGGER.debug("rootdir   is " + rootDirectoryString);
        LOGGER.debug("searchdir is " + searchDirectoryString);
        showKeys("keys before filter", keys);
        List<BucketPath> result = new ArrayList<>();

        // showKeys(keys);
        int numberOfDelimitersOfPrefix = StringUtils.countMatches(prefix, BucketPath.BUCKET_SEPARATOR);
        if (searchDirectoryString.length() > BucketPath.BUCKET_SEPARATOR.length()) {
            numberOfDelimitersOfPrefix++;
        }
        int numberOfDelimitersExpected = numberOfDelimitersOfPrefix;

        keys.forEach(key -> {
            if (recursive.equals(ListRecursiveFlag.TRUE)) {
                String keyWithoutPrefix = key.substring(prefix.length());
                result.add(searchDirectory.appendName(keyWithoutPrefix));
            } else {
                int numberOfDelimitersOfKey = StringUtils.countMatches(key, BucketPath.BUCKET_SEPARATOR);
                if (numberOfDelimitersOfKey == numberOfDelimitersExpected) {
                    String keyWithoutPrefix = key.substring(prefix.length());
                    result.add(searchDirectory.appendName(keyWithoutPrefix));
                }
            }
        });
        showResult("after filter", result);
        return result;
    }


    private void putBlobStreamWithMemory(BucketPath abucketPath, PayloadStream payloadStream, int size) {
        try {
            LOGGER.debug("putBlobStreamWithMemory " + abucketPath + " with known length " + size);
            ObjectMetadata objectMetadata = new ObjectMetadata();
            objectMetadata.setContentLength(size);

            BucketPath bucketPath = amazonS3RootBucket.append(abucketPath);

            PutObjectRequest putObjectRequest = new PutObjectRequest(
                    bucketPath.getContainer(),
                    bucketPath.getName(),
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

                PutObjectRequest putObjectRequest = new PutObjectRequest(bucketPath.getContainer(), bucketPath.getName(), fis, objectMetadata);
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
        BucketDirectory bucketDirectory = amazonS3RootBucket.append(abucketDirectory);

        String container = bucketDirectory.getContainer();
        String prefix = bucketDirectory.getName();
        if (prefix == null) {
            prefix = "";
        }
        LOGGER.debug("listObjects(" + container + "," + prefix + ")");

        int totalCount = 0;
        ObjectListing ol = connection.listObjects(container, prefix);
        LOGGER.debug(bucketDirectory + " contains " + ol.getObjectSummaries().size() + " elements that should be deleted");
        while (!ol.getObjectSummaries().isEmpty()) {

            int CHUNKSIZE = 100;
            List<DeleteObjectsRequest.KeyVersion> keys = new ArrayList<>();
            for (S3ObjectSummary key : ol.getObjectSummaries()) {
                keys.add(new DeleteObjectsRequest.KeyVersion(key.getKey()));
                if (keys.size() == CHUNKSIZE) {
                    DeleteObjectsRequest deleteObjectsRequest = new DeleteObjectsRequest(container);
                    deleteObjectsRequest.setKeys(keys);
                    LOGGER.debug(bucketDirectory + " chunk with " + keys.size() + " elements will be deleted");
                    DeleteObjectsResult deleteObjectsResult = connection.deleteObjects(deleteObjectsRequest);
                    LOGGER.debug(bucketDirectory + " deletion of chunk with " + deleteObjectsResult.getDeletedObjects().size() + " elements confirmed");
                    totalCount += keys.size();
                    keys.clear();
                }
            }
            if (!keys.isEmpty()) {
                DeleteObjectsRequest deleteObjectsRequest = new DeleteObjectsRequest(container);
                deleteObjectsRequest.setKeys(keys);
                LOGGER.debug(bucketDirectory + " chunk with " + keys.size() + " elements will be deleted");
                DeleteObjectsResult deleteObjectsResult = connection.deleteObjects(deleteObjectsRequest);
                totalCount += keys.size();
                LOGGER.debug(bucketDirectory + " deletion of chunk with " + deleteObjectsResult.getDeletedObjects().size() + " elements confirmed");
            }

            ol = connection.listObjects(container, prefix);
            if (!ol.getObjectSummaries().isEmpty()) {
                LOGGER.debug(bucketDirectory + " still contains " + ol.getObjectSummaries().size() + " elements that should be deleted. repeat deletion");
            }
        }
        LOGGER.info(bucketDirectory + " total elements deleted:" + totalCount);
    }

    private void showResult(String message, List<BucketPath> result) {
        LOGGER.debug(message);
        result.forEach(el -> {
            LOGGER.debug(el.toString());
        });
    }

    private void showKeys(String message, List<String> keys) {
        LOGGER.debug(message);
        keys.forEach(el -> {
            LOGGER.debug(el);
        });
    }

}
