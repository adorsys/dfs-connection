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
import de.adorsys.common.utils.HexUtil;
import de.adorsys.dfs.connection.api.types.connection.AmazonS3AccessKey;
import de.adorsys.dfs.connection.api.types.connection.AmazonS3SecretKey;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.*;


@SuppressWarnings("Duplicates")
/**
 * Created by peter on 19.09.18.
 * Dieser Test kann direkt für Ceph, Amazon oder Minio benutzt werden. Er ist aber auskommentiert,
 * da hier die Credentials fest verdrahtet sind.
 */
public class DirectAmazonS3Test {
    private final static Logger LOGGER = LoggerFactory.getLogger(DirectAmazonS3Test.class);
    private AmazonS3AccessKey accessKey = new AmazonS3AccessKey("*");
    private AmazonS3SecretKey secretKey = new AmazonS3SecretKey("*");
    private String urlString = "https://s3.amazonaws.com";
    private String rootbucket = "adorsys-docusafe";
    private String region = "eu-central-1";
    private String DELIMITER = "/";

    private static URL getUrl(String url) {
        try {
            return new URL(url);
        } catch (Exception e) {
            throw BaseExceptionHandler.handle(e);
        }
    }

    // @Test
    public void justASnipletTest() {
        try {
            String AFFE = rootbucket;
            AWSCredentialsProvider credentialsProvider = new AWSCredentialsProvider() {
                @Override
                public AWSCredentials getCredentials() {
                    return new BasicAWSCredentials(accessKey.getValue(), secretKey.getValue());
                }

                @Override
                public void refresh() {

                }
            };
            ClientConfiguration configuration = new ClientConfiguration();
            configuration.setProtocol(Protocol.HTTP);

            // AwsClientBuilder.EndpointConfiguration endpoint = new AwsClientBuilder.EndpointConfiguration(urlString, region);

            ClientConfiguration clientConfig = new ClientConfiguration();
            clientConfig.setSocketTimeout(500);
            clientConfig.setProtocol(Protocol.HTTP);
            clientConfig.disableSocketProxy();
            AmazonS3 conn = AmazonS3ClientBuilder.standard()
                    .withCredentials(credentialsProvider)
               //     .withEndpointConfiguration(endpoint)
                    .withRegion(region)
                    .withClientConfiguration(clientConfig)
                    .withPayloadSigningEnabled(false)
                    .enablePathStyleAccess()
                    .build();
            boolean allowedToSeeBuckets = false;
            if (allowedToSeeBuckets) {
                List<Bucket> buckets = conn.listBuckets();
                buckets.forEach(bucket -> {
                    LOGGER.debug("found bucket:" + bucket);
                    ObjectListing objectListing = conn.listObjects(new ListObjectsRequest().withBucketName(bucket.getName()));
                    objectListing.getObjectSummaries().forEach(sum -> {
                        LOGGER.debug("found " + sum.getKey() + " in " + sum.getBucketName());
                        VersionListing versionListing = conn.listVersions(sum.getBucketName(), sum.getKey());
                        versionListing.getVersionSummaries().forEach(vsum -> {
                            LOGGER.debug("key:" + vsum.getKey() + " version:" + vsum.getVersionId());
                        });
                    });
                });
            }

            // Erzeuge Datei affe.txt im bucket affe
            Bucket bucket = null;
            if (!conn.doesBucketExistV2(AFFE)) {
                LOGGER.debug("bucket " + AFFE + " does not exist yet. create it");
                bucket = conn.createBucket(AFFE);
            } else {
                LOGGER.debug("bucket " + AFFE + " wird wiederverwendet");
                if (allowedToSeeBuckets) {
                    bucket = conn.listBuckets().stream().filter(b -> b.getName().equals(AFFE)).findFirst().get();
                }
            }

            String key = "firstFile.txt";
            LOGGER.debug("start create file " + key + " in bucket " + AFFE);
            String content = "affe";
            InputStream is = new ByteArrayInputStream(content.getBytes());

            {
                LOGGER.debug("safe directly");
                ObjectMetadata objectMetadata = new ObjectMetadata();
//                 objectMetadata.setContentLength(content.getBytes().length);
                Map<String, String> userMetadata = new HashMap<>();
                userMetadata.put("mykey", "myvalue");
                userMetadata.put("mykey2", "{\"jsonkey\":\"json myvalue\"}");

                objectMetadata.setUserMetadata(userMetadata);
                PutObjectRequest putObjectRequest = new PutObjectRequest(AFFE, key, is, objectMetadata);
                PutObjectResult putObjectResult = conn.putObject(putObjectRequest);
                LOGGER.debug("creation of object :" + putObjectResult.toString());
            }
            {
                LOGGER.debug("read written file");
                GetObjectRequest getObjectRequest = new GetObjectRequest(AFFE, key);
                S3Object object = conn.getObject(getObjectRequest);
                S3ObjectInputStream objectContent = object.getObjectContent();
                String readContent = IOUtils.toString(objectContent, Charset.defaultCharset());
                if (!readContent.equals(content)) {
                    throw new BaseException("geschrieben wurde:" + content + " aber gelesen wurde " + readContent);
                }
                LOGGER.debug("Inhalt wurde korrekt ausgelesen");
                ObjectMetadata objectMetadata = object.getObjectMetadata();
                LOGGER.debug("version id:" + objectMetadata.getVersionId());
                objectMetadata.getUserMetadata().keySet().forEach(mkey -> {
                    LOGGER.debug("key " + mkey + " value:" + objectMetadata.getUserMetadata().get(mkey));
                });
            }
            {
                LOGGER.debug("vor delete exists:" + exists(conn, AFFE, key));
                conn.deleteObject(AFFE, key);
                LOGGER.debug("nach delete exists:" + exists(conn, AFFE, key));
            }
            {
                createFile(conn, AFFE, "FILE1.TXT");
                createFile(conn, AFFE, "FILE2.TXT");
                createFile(conn, AFFE, "dir1/FILE1.TXT");
                createFile(conn, AFFE, "dir1/FILE2.TXT");
                createFile(conn, AFFE, "dir2/FILE1.TXT");
                createFile(conn, AFFE, "dir2/FILE2.TXT");
                createFile(conn, AFFE, "dir1/dir11/FILE1.TXT");
                createFile(conn, AFFE, "dir1/dir11/FILE2.TXT");
                createFile(conn, AFFE, "dir1/dir12/FILE1.TXT");
                createFile(conn, AFFE, "dir1/dir12/FILE2.TXT");
                createFile(conn, AFFE, "dir2/dir21/FILE1.TXT");
                createFile(conn, AFFE, "dir2/dir21/FILE2.TXT");
                createFile(conn, AFFE, "dir2/dir22/FILE1.TXT");
                createFile(conn, AFFE, "dir2/dir22/FILE2.TXT");
            }
            LOGGER.debug("-----------------------------------------------");
            showRecursive(conn, AFFE, "/");
            showRecursive(conn, AFFE, "/dir1");
            showRecursive(conn, AFFE, "/dir2/dir22");


        } catch (Exception e) {
            throw BaseExceptionHandler.handle(e);
        }
    }

    void showRecursive(AmazonS3 conn, String container, String fullprefix) {
        if (!fullprefix.startsWith(DELIMITER)) {
            throw new BaseException("can not find starting delimiter in " + fullprefix);
        }
        String prefix = fullprefix.substring(1);
        ObjectListing ol = conn.listObjects(container, prefix);
        final List<String> keys = new ArrayList<>();
        ol.getObjectSummaries().forEach(el -> keys.add(DELIMITER + el.getKey()));
        keys.forEach(key -> LOGGER.debug("BEFORE found for " + fullprefix + " :" + key));
        LOGGER.debug("--");
        LOGGER.debug("in total:" + keys.size());
        {
            List<String> fiteredkeys = filter(fullprefix, keys, false);
            fiteredkeys.forEach(key -> LOGGER.debug("AFTER non recursive found for " + fullprefix + " :" + key));
            LOGGER.debug("in total:" + fiteredkeys.size());
        }
        {
            List<String> fiteredkeys = filter(fullprefix, keys, true);
            fiteredkeys.forEach(key -> LOGGER.debug("AFTER     recursive found for " + fullprefix + " :" + key));
            LOGGER.debug("in total:" + fiteredkeys.size());
        }

    }

    boolean exists(AmazonS3 conn, String bucket, String name) {
        try {
            conn.getObjectMetadata(bucket, name);
            return true;
        } catch (Exception e) {
            return false;
        }

    }

    List<String> filter(String prefix, final List<String> keys, boolean recursive) {
        List<String> newkeys = new ArrayList<>();
        Set<String> dirs = new HashSet<>();

        int numberOfDelimitersOfPrefix = StringUtils.countMatches(prefix, DELIMITER);
        if (prefix.length() > DELIMITER.length()) {
            numberOfDelimitersOfPrefix++;
        }
        int numberOfDelimitersExpected = numberOfDelimitersOfPrefix;

        keys.forEach(key -> {
            if (recursive) {
                newkeys.add(key);
            } else {
                int numberOfDelimitersOfKey = StringUtils.countMatches(key, DELIMITER);
                if (numberOfDelimitersOfKey == numberOfDelimitersExpected) {
                    newkeys.add(key);
                }
            }

            if (recursive) {
                int lastDelimiter = key.lastIndexOf(DELIMITER);
                String dir = key.substring(0, lastDelimiter);
                dirs.add(dir + " (DIR)");
            } else {
                int numberOfDelimitersOfKey = StringUtils.countMatches(key, DELIMITER);
                if (numberOfDelimitersOfKey == numberOfDelimitersExpected + 1) {
                    int lastDelimiter = key.lastIndexOf(DELIMITER);
                    String dir = key.substring(0, lastDelimiter);
                    dirs.add(dir + " (DIR)");
                }
            }

        });
        newkeys.addAll(dirs);
        return newkeys;
    }

    List<String> filterOld(String prefix, final List<String> keys, boolean recursive) {
        List<String> newkeys = new ArrayList<>();
        Set<String> dirs = new HashSet<>();

        int numberOfDelimiters = StringUtils.countMatches(prefix, DELIMITER);
        if (recursive) {
            if (prefix.length() >= DELIMITER.length()) {
                numberOfDelimiters++;
            }
        }
        int numberOfExpectedDelimiters = numberOfDelimiters;
        keys.forEach(key -> {
            if (recursive) {
                newkeys.add(key);
            } else {
                int numberOfKeyDelemiters = StringUtils.countMatches(key, DELIMITER);
                if (numberOfKeyDelemiters == numberOfExpectedDelimiters) {
                    newkeys.add(key);
                }
            }
            if (recursive) {
                int lastDelimiter = key.lastIndexOf(DELIMITER);
                String dir = key.substring(0, lastDelimiter);
                dirs.add(dir);
            } else {
                int numberOfKeyDelemiters = StringUtils.countMatches(key, DELIMITER);
                if (numberOfKeyDelemiters == numberOfExpectedDelimiters + 1) {
                    int lastDelimiter = key.lastIndexOf(DELIMITER);
                    String dir = key.substring(0, lastDelimiter);
                    dirs.add(dir);
                }

            }

        });
        newkeys.addAll(dirs);
        return newkeys;
    }

    void createFile(AmazonS3 conn, String bucket, String name) {
        LOGGER.debug("safe directly file " + bucket + " " + name);
        String content = "content of " + bucket + " " + name;
        InputStream is = new ByteArrayInputStream(content.getBytes());
        ObjectMetadata objectMetadata = new ObjectMetadata();
        PutObjectRequest putObjectRequest = new PutObjectRequest(bucket, name, is, objectMetadata);
        objectMetadata.setContentLength(content.getBytes().length);
        PutObjectResult putObjectResult = conn.putObject(putObjectRequest);
        LOGGER.debug("creation of object :" + putObjectResult.toString());
    }
}
