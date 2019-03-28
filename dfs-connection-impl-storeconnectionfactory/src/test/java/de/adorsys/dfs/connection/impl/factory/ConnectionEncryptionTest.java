package de.adorsys.dfs.connection.impl.factory;

import de.adorsys.common.exceptions.BaseException;
import de.adorsys.dfs.connection.api.complextypes.BucketDirectory;
import de.adorsys.dfs.connection.api.complextypes.BucketPath;
import de.adorsys.dfs.connection.api.complextypes.BucketPathUtil;
import de.adorsys.dfs.connection.api.exceptions.BucketRestrictionException;
import de.adorsys.dfs.connection.api.service.api.ExtendedStoreConnection;
import de.adorsys.dfs.connection.api.types.ListRecursiveFlag;
import de.adorsys.dfs.connection.api.types.properties.BucketPathEncryptionFilenameOnly;
import de.adorsys.dfs.connection.api.types.properties.ConnectionProperties;
import de.adorsys.dfs.connection.api.types.properties.ConnectionPropertiesImpl;
import de.adorsys.dfs.connection.impl.amazons3.AmazonS3ExtendedStoreConnection;
import de.adorsys.dfs.connection.impl.pathencryption.BucketPathEncryption;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by peter on 10.10.18 09:57.
 */
public class ConnectionEncryptionTest {
    private final static Logger LOGGER = LoggerFactory.getLogger(ConnectionEncryptionTest.class);
    public static Set<BucketDirectory> plainbuckets = new HashSet<>();
    public static Set<BucketDirectory> encryptedbuckets = new HashSet<>();

    private ExtendedStoreConnection extendedStoreConnectionWithEncryption;
    private ExtendedStoreConnection extendedStoreConnectionWithoutEncryption;

    @Before
    public void before() {
        ConnectionProperties connectionProperties = new ReadArguments().readEnvironment();
        ((ConnectionPropertiesImpl) connectionProperties).setBucketPathEncryptionPassword(null);
        extendedStoreConnectionWithoutEncryption = ExtendedStoreConnectionFactory.get(connectionProperties);

        ((ConnectionPropertiesImpl) connectionProperties).setBucketPathEncryptionPassword(ConnectionProperties.defaultEncryptionPassword);
        extendedStoreConnectionWithEncryption = ExtendedStoreConnectionFactory.get(connectionProperties);

        // TestKeyUtils.turnOffEncPolicy();
        plainbuckets.clear();
        encryptedbuckets.clear();
    }

    @After
    public void after() {
        for (BucketDirectory bucket : plainbuckets) {
            try {
                extendedStoreConnectionWithoutEncryption.deleteContainer(bucket);
            } catch (Exception e) {
                LOGGER.error("AFTER TEST: PROBLEM DELETING BUCKET (in unencrypted version) " + bucket);
                // ignore Exception
            }
        }

        for (BucketDirectory bucket : encryptedbuckets) {
            try {
                extendedStoreConnectionWithEncryption.deleteContainer(bucket);
            } catch (Exception e) {
                LOGGER.error("AFTER TEST: PROBLEM DELETING BUCKET (in encrypted version) " + bucket);
                // ignore Exception
            }
        }
    }

    @Test
    public void findMaxLengthEncypted() {
        ExtendedStoreConnection extendedStoreConnection = extendedStoreConnectionWithEncryption;
        String keystore = "keystore";
        String toolong = "dasisteinsehrlangesverzeichnisundwennesverschluesseltwirddannistesnochviellaenger1235678j90asdflqqmj";
        toolong = toolong + toolong; // 200 Zeichen

        BucketPath lastValidFilePath = null;
        int length = 10;

        boolean bucketRestrictionExceptionCaught = false;
        boolean exceptionCaught = false;

        while (!exceptionCaught) {
            try {
                String s = toolong.substring(0, length);
                BucketDirectory bd = new BucketDirectory(s);
                extendedStoreConnection.createContainer(bd);
                encryptedbuckets.add(bd);
                extendedStoreConnection.list(bd, ListRecursiveFlag.TRUE);
                BucketPath filePath = bd.appendName(keystore);
                extendedStoreConnection.putBlob(filePath, "10".getBytes());
                extendedStoreConnection.removeBlob(filePath);
                extendedStoreConnection.deleteContainer(filePath.getBucketDirectory());
                encryptedbuckets.remove(bd);
                lastValidFilePath = filePath;
                length++;
                if (length > toolong.length()) {
                    exceptionCaught = true;
                    LOGGER.info("max länge erreicht");

                }
            } catch (BucketRestrictionException e) {
                LOGGER.info("BucketRestrictionExceptoin Expected");
                exceptionCaught = true;
                bucketRestrictionExceptionCaught = true;
            } catch (Exception e) {
                new BaseException("see stack that finished search:", e);
                exceptionCaught = true;
            }
        }
        LOGGER.info("the directory name length is " + lastValidFilePath.getObjectHandle().getContainer().length() + " " + lastValidFilePath);
        LOGGER.info("the encrypted name length is " + BucketPathEncryption.encrypt(ConnectionProperties.defaultEncryptionPassword, BucketPathEncryptionFilenameOnly.FALSE, lastValidFilePath).getObjectHandle().getContainer().length() + " " +
                BucketPathEncryption.encrypt(ConnectionProperties.defaultEncryptionPassword, BucketPathEncryptionFilenameOnly.FALSE, lastValidFilePath));
        Assert.assertTrue(bucketRestrictionExceptionCaught);
    }

    @Test
    public void findMaxLengthUnEncypted() {
        ExtendedStoreConnection extendedStoreConnection = extendedStoreConnectionWithoutEncryption;
        String keystore = "16a734123a5a949fc2cbaef8fca7d36faffe";
        String toolong = "17c6ddd04fa11c324c0fd091aae10632c91e59aaecfd0a5fcbb4e380b49b87e78baaa3454c579817d2056f25516ec8dde25a529ebc56db558bbbdf2718255f9898b024bdb05dcbb9a29c8b189cc8b0d9";
        toolong = toolong + toolong;

        int length = 10;
        BucketPath lastValidFilePath = null;
        boolean bucketRestrictionExceptionCaught = false;
        boolean exceptionCaught = false;

        while (!exceptionCaught) {
            try {
                exceptionCaught = false;
                String s = toolong.substring(0, length);
                BucketDirectory bd = new BucketDirectory(s);
                extendedStoreConnection.createContainer(bd);
                plainbuckets.add(bd);
                extendedStoreConnection.list(bd, ListRecursiveFlag.TRUE);
                BucketPath filePath = bd.appendName(keystore);
                extendedStoreConnection.putBlob(filePath, "10".getBytes());
                extendedStoreConnection.removeBlob(filePath);
                extendedStoreConnection.deleteContainer(filePath.getBucketDirectory());
                plainbuckets.remove(bd);
                lastValidFilePath = filePath;
                length++;
                if (length > toolong.length()) {
                    exceptionCaught = true;
                    LOGGER.info("max länge erreicht");
                }
            } catch (BucketRestrictionException e) {
                LOGGER.info("expected BucketRestrictionException");
                bucketRestrictionExceptionCaught = true;
                exceptionCaught = true;
            } catch (Exception e) {
                new BaseException("see stack that finished search:", e);
                exceptionCaught = true;
            }
        }
        LOGGER.info("the directory name length is " + lastValidFilePath.getObjectHandle().getContainer().length() + " " + lastValidFilePath);
        Assert.assertTrue(bucketRestrictionExceptionCaught);
    }

    // @Test
    public void findPathTotalLimit() {
        ExtendedStoreConnection extendedStoreConnection = extendedStoreConnectionWithEncryption;
        String folder = "affe";
        int repeats = 0;
        int maxrepeats = 20;
        BucketPath lastValidFilePath = null;
        BucketPath filePath = null;
        boolean exceptionCaught = false;
        while (!exceptionCaught) {
            try {
                BucketDirectory start = new BucketDirectory(folder);
                BucketDirectory bd = start;

                for (int i = 0; i < repeats; i++) {
                    bd = bd.appendDirectory(folder);
                }
                filePath = bd.appendName("file.txt");

                extendedStoreConnection.createContainer(bd);
                encryptedbuckets.add(bd);
                extendedStoreConnection.list(bd, ListRecursiveFlag.TRUE);
                extendedStoreConnection.putBlob(filePath, "10".getBytes());
                extendedStoreConnection.removeBlob(filePath);
                extendedStoreConnection.deleteContainer(filePath.getBucketDirectory());
                encryptedbuckets.remove(bd);
                lastValidFilePath = filePath;
                LOGGER.info("the last successfull encrypted name length is " + BucketPathUtil.getAsString(BucketPathEncryption.encrypt(ConnectionProperties.defaultEncryptionPassword, BucketPathEncryptionFilenameOnly.FALSE, lastValidFilePath)).length());
                repeats++;
                if (repeats == maxrepeats) {
                    exceptionCaught = true;
                }
            } catch (Exception e) {
                LOGGER.info("the failed encrypted name length is " + BucketPathUtil.getAsString(BucketPathEncryption.encrypt(ConnectionProperties.defaultEncryptionPassword, BucketPathEncryptionFilenameOnly.FALSE, filePath)).length());
                new BaseException("see stack that finished search:", e);
                exceptionCaught = true;
            }
        }
        LOGGER.info("absolute name length is      " + BucketPathUtil.getAsString(lastValidFilePath).length());
        LOGGER.info("the encrypted name length is " + BucketPathUtil.getAsString(BucketPathEncryption.encrypt(ConnectionProperties.defaultEncryptionPassword, BucketPathEncryptionFilenameOnly.FALSE, lastValidFilePath)).length());
    }

    // @Test
    public void findPathTotalLimitInDetail() {
        ExtendedStoreConnection extendedStoreConnection = extendedStoreConnectionWithoutEncryption;
        String folder = "1b05be01a233fe6dcf46e5972dce79a2";
        int repeats = 4;
        int length = 0;
        int maxlength = 50;
        BucketPath lastValidFilePath = null;
        BucketPath filePath = null;
        boolean exceptionCaught = false;
        while (!exceptionCaught) {
            try {
                BucketDirectory start = new BucketDirectory(folder);
                BucketDirectory bd = start;

                for (int i = 0; i < repeats; i++) {
                    bd = bd.appendDirectory(folder);
                }
                String name = "a";
                for (int j = 0; j < length; j++) {
                    name = name + "b";
                }
                filePath = bd.appendName(name);

                extendedStoreConnection.createContainer(bd);
                encryptedbuckets.add(bd);
                extendedStoreConnection.list(bd, ListRecursiveFlag.TRUE);
                extendedStoreConnection.putBlob(filePath, "10".getBytes());
                extendedStoreConnection.removeBlob(filePath);
                extendedStoreConnection.deleteContainer(filePath.getBucketDirectory());
                encryptedbuckets.remove(bd);
                lastValidFilePath = filePath;
                LOGGER.info("the last successfull encrypted name length is " + BucketPathUtil.getAsString(BucketPathEncryption.encrypt(ConnectionProperties.defaultEncryptionPassword, BucketPathEncryptionFilenameOnly.FALSE, lastValidFilePath)).length());
                length++;
                if (length == maxlength) {
                    exceptionCaught = true;
                }
            } catch (Exception e) {
                LOGGER.info("the failed encrypted name length is " + BucketPathUtil.getAsString(BucketPathEncryption.encrypt(ConnectionProperties.defaultEncryptionPassword, BucketPathEncryptionFilenameOnly.FALSE, filePath)).length());
                new BaseException("see stack that finished search:", e);
                exceptionCaught = true;
            }
        }
        LOGGER.info("absolute name length is      " + BucketPathUtil.getAsString(lastValidFilePath).length());
        LOGGER.info("absolute name is             " + BucketPathUtil.getAsString(lastValidFilePath));
    }

    // @Test
    public void cleanDatabase() {
        cleanDatabase(extendedStoreConnectionWithoutEncryption);
        cleanDatabase(extendedStoreConnectionWithEncryption);
    }

    private void cleanDatabase(ExtendedStoreConnection extendedStoreConnection) {
        if (extendedStoreConnection instanceof AmazonS3ExtendedStoreConnection) {
            ((AmazonS3ExtendedStoreConnection) extendedStoreConnection).cleanDatabase();
        } else {
            extendedStoreConnection.listAllBuckets().forEach(bucket -> extendedStoreConnection.deleteContainer(bucket));
        }
    }


}