package de.adorsys.dfs.connection.impl.factory;

import de.adorsys.common.exceptions.BaseException;
import de.adorsys.common.exceptions.BaseExceptionHandler;
import de.adorsys.common.utils.HexUtil;
import de.adorsys.dfs.connection.api.complextypes.BucketDirectory;
import de.adorsys.dfs.connection.api.complextypes.BucketPath;
import de.adorsys.dfs.connection.api.domain.Payload;
import de.adorsys.dfs.connection.api.domain.PayloadStream;
import de.adorsys.dfs.connection.api.exceptions.StorageConnectionException;
import de.adorsys.dfs.connection.api.filesystem.FilesystemConnectionPropertiesImpl;
import de.adorsys.dfs.connection.api.service.api.DFSConnection;
import de.adorsys.dfs.connection.api.service.impl.SimplePayloadImpl;
import de.adorsys.dfs.connection.api.service.impl.SimplePayloadStreamImpl;
import de.adorsys.dfs.connection.api.types.ListRecursiveFlag;
import de.adorsys.dfs.connection.api.types.connection.AmazonS3RootBucketName;
import de.adorsys.dfs.connection.api.types.connection.FilesystemRootBucketName;
import de.adorsys.dfs.connection.api.types.properties.ConnectionProperties;
import de.adorsys.dfs.connection.impl.amazons3.AmazonS3ConnectionProperitesImpl;
import de.adorsys.dfs.connection.impl.amazons3.AmazonS3DFSConnection;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by peter on 06.02.18 at 16:45.
 */
public class DFSConnectionTest {
    private final static Logger LOGGER = LoggerFactory.getLogger(DFSConnectionTest.class);

    private List<BucketDirectory> containers = new ArrayList<>();
    private DFSConnection s = DFSConnectionFactory.get();

    @Before
    public void before() {
        containers.clear();
    }

    @After
    public void after() {
        for (BucketDirectory c : containers) {
            try {
                LOGGER.debug("AFTER TEST DELETE CONTAINER " + c);
                s.deleteContainer(c);
            } catch (Exception e) {
                // ignore
            }
        }
    }

    @Test
    public void cleanDB() {
        DFSConnection c = DFSConnectionFactory.get();
        c.listAllBuckets().forEach(el -> c.deleteContainer(el));
    }

    /*
    This test requrires manual access
     */
    // @Test
    public void testConnectionAvaiable() {
        cleanDB();
        BucketDirectory bd = new BucketDirectory("test-container-exists");
        containers.add(bd);

        Assert.assertFalse(s.containerExists(bd));
        s.createContainer(bd);
        try {
            LOGGER.debug("you have 10 secs to kill the connection");
            Thread.currentThread().sleep(10000);
            LOGGER.debug("continue");
        } catch (Exception e) {
            throw BaseExceptionHandler.handle(e);
        }

        Assert.assertTrue(s.containerExists(bd));
        containers.add(bd);

        BucketPath file = bd.appendName("file.txt");
        Assert.assertFalse(s.blobExists(file));

        byte[] filecontent = "Inhalt".getBytes();
        s.putBlob(file, new SimplePayloadImpl(filecontent));

        try {
            LOGGER.debug("you have 10 secs to kill the connection");
            Thread.currentThread().sleep(10000);
            LOGGER.debug("continue");
        } catch (Exception e) {
            throw BaseExceptionHandler.handle(e);
        }

        Assert.assertTrue(s.blobExists(file));

    }

    @Test
    public void testListSubfolderRecursive() {
        BucketDirectory bd = new BucketDirectory("test_list_subfolder");
        s.createContainer(bd);
        containers.add(bd);

        BucketPath file = bd.appendName("/empty1/empty2/file.txt");

        byte[] filecontent = "Inhalt".getBytes();
        s.putBlob(file, new SimplePayloadImpl(filecontent));

        {
            List<BucketPath> files = s.list(bd, ListRecursiveFlag.TRUE);
            showBucketPath("list", files);
            Assert.assertEquals(1, files.size());
            Assert.assertTrue(s.blobExists(file));
        }

        {
            List<BucketPath> files = s.list(bd.appendDirectory("empty1"), ListRecursiveFlag.TRUE);
            showBucketPath("list", files);
            Assert.assertEquals(1, files.size());
            Assert.assertTrue(s.blobExists(file));
        }

        {
            List<BucketPath> files = s.list(bd.appendDirectory("empty1/empty2"), ListRecursiveFlag.TRUE);
            showBucketPath("list", files);
            Assert.assertEquals(1, files.size());
            Assert.assertTrue(s.blobExists(file));
        }
    }

    @Test
    public void testListSubfolderNonRecursive() {
        BucketDirectory bd = new BucketDirectory("test_list_subfolder");
        s.createContainer(bd);
        containers.add(bd);

        BucketPath file = bd.appendName("/empty1/empty2/file.txt");

        byte[] filecontent = "Inhalt".getBytes();
        s.putBlob(file, new SimplePayloadImpl(filecontent));

        {
            List<BucketPath> files = s.list(bd, ListRecursiveFlag.FALSE);
            showBucketPath("list", files);
            Assert.assertEquals(0, files.size());
            Assert.assertTrue(s.blobExists(file));
        }
        {
            List<BucketPath> files = s.list(bd.appendDirectory("empty1"), ListRecursiveFlag.FALSE);
            showBucketPath("list", files);
            Assert.assertEquals(0, files.size());
            Assert.assertTrue(s.blobExists(file));
        }
        {
            List<BucketPath> files = s.list(bd.appendDirectory("empty1/empty2"), ListRecursiveFlag.FALSE);
            showBucketPath("list", files);

            Assert.assertEquals(1, files.size());
            Assert.assertTrue(s.blobExists(file));
        }
    }

    /**
     * Suche in einem nicht vorhandenem Bucket sollte einfach eine leere Liste zurückgeben
     */
    @Test
    public void testList1() {
        List<BucketPath> files = s.list(new BucketDirectory("abc"), ListRecursiveFlag.FALSE);

        Assert.assertEquals(0, files.size());
    }

    /**
     * Liste eines echten Containers sollte genau ein Directory zurückliefern
     */
    @Test
    public void testList2() {
        BucketDirectory bd = new BucketDirectory("affe2");
        s.createContainer(bd);
        containers.add(bd);

        List<BucketPath> files = s.list(bd, ListRecursiveFlag.FALSE);
        showBucketPath("list", files);
        Assert.assertEquals(0, files.size());
    }

    /**
     * Liste einer Datei sollte genau diese mit zurückliefern
     */
    @Test
    public void testList3() {
        int REPEATS = 10;
        int i = 0;

        while (i > 0) {
            LOGGER.debug("wait for visualVM profiler " + i);
            try {
                Thread.currentThread().sleep(1000);
            } catch (Exception e) {
            }
            i--;
        }
        BucketDirectory bd = new BucketDirectory("affe3");
        s.createContainer(bd);
        containers.add(bd);
        for (int j = 0; j < REPEATS; j++) {
            BucketPath file = bd.appendName("file1");
            if (s.blobExists(file)) {
                s.removeBlob(file);
            }

            byte[] filecontent = "Inhalt".getBytes();
            s.putBlob(file, new SimplePayloadImpl(filecontent));

            List<BucketPath> files = s.list(bd, ListRecursiveFlag.FALSE);
            showBucketPath("list", files);

            Assert.assertEquals(1, files.size());
            Assert.assertTrue(s.blobExists(file));

            Payload loadedPayload = s.getBlob(file);
            byte[] loadedFileContent = loadedPayload.getData();
            Assert.assertTrue(Arrays.equals(filecontent, loadedFileContent));
        }
    }

    /**
     * Kein Unterverzeichnis, nur der Bucket.
     * Ein nicht existentes Directory darf keinen Fehler verursachen
     * so ist es zumindes bei der jclouldFilesystem umsetzung
     */
    @Test
    public void testList4() {
        BucketDirectory bd = new BucketDirectory("affe4");

        List<BucketPath> files = s.list(bd, ListRecursiveFlag.FALSE);
        showBucketPath("list", files);

        Assert.assertEquals(0, files.size());
    }

    /**
     * Wenn als Verzeichnis eine Datei angegeben wird, dann muss eine leere Liste
     * zurückkommen, so zuindest verhält sich jcloud
     */
    @Test
    public void testList5() {
        BucketDirectory bd = new BucketDirectory("affe5");
        s.createContainer(bd);
        containers.add(bd);

        BucketPath file = bd.appendName("file1");
        s.putBlob(file, new SimplePayloadImpl("Inhalt".getBytes()));
        BucketDirectory bdtrick = new BucketDirectory(file);
        List<BucketPath> files = s.list(bdtrick, ListRecursiveFlag.FALSE);

        Assert.assertEquals(0, files.size());
    }


    /**
     * recursive search finds all files
     * non recursive search finds files of current directory
     */
    @Test
    public void testList6() {
        BucketDirectory bd = new BucketDirectory("affe6/1/2/3");
        s.createContainer(bd);
        containers.add(bd);

        s.putBlob(bd.append(new BucketPath("filea")), new SimplePayloadImpl("Inhalt".getBytes()));
        s.putBlob(bd.append(new BucketPath("fileb")), new SimplePayloadImpl("Inhalt".getBytes()));
        s.putBlob(bd.append(new BucketPath("subdir1/filec")), new SimplePayloadImpl("Inhalt".getBytes()));
        s.putBlob(bd.append(new BucketPath("subdir1/filed")), new SimplePayloadImpl("Inhalt".getBytes()));
        List<BucketPath> files = s.list(bd, ListRecursiveFlag.TRUE);
        showBucketPath("recursive", files);
        {
            Assert.assertEquals(4, files.size());
        }


        files = s.list(bd, ListRecursiveFlag.FALSE);
        showBucketPath("plain", files);
        {
            Assert.assertEquals(2, files.size());
        }
    }

    /**
     * Nun mit Prüfung, dass auch wirklich die vorhandenen Dateien gefunden werden
     */
    @Test
    public void testList7() {
        BucketDirectory bd = new BucketDirectory("affe7/1/2/3");
        s.createContainer(bd);
        containers.add(bd);

        s.putBlob(bd.append(new BucketPath("subdir1/filea")), new SimplePayloadImpl("Inhalt".getBytes()));
        List<BucketPath> files = s.list(bd, ListRecursiveFlag.TRUE);
        showBucketPath("list", files);
        Assert.assertTrue(files.contains(new BucketPath("affe7/1/2/3/subdir1/filea")));
        Assert.assertEquals(1, files.size());

        files = s.list(bd, ListRecursiveFlag.FALSE);
        showBucketPath("list", files);
        Assert.assertEquals(0, files.size());
    }


    @Test
    public void deleteDatabase() {
        if (s instanceof AmazonS3DFSConnection) {
            ((AmazonS3DFSConnection) s).cleanDatabase();
        }
    }

    @Test
    public void testDeleteFolder() {
        LOGGER.debug("START TEST " + new RuntimeException("").getStackTrace()[0].getMethodName());
        BucketDirectory bd = new BucketDirectory("deletedeep");
        s.createContainer(bd);
        containers.add(bd);

        /**
         * Anlegen einer tieferen Verzeichnisstruktur
         */
        createFiles(s, bd, 2, 2, 5);

        if (s instanceof AmazonS3DFSConnection) {
            ((AmazonS3DFSConnection) s).showDatabase();
        }

        List<BucketPath> filesOnlyAll = s.list(bd, ListRecursiveFlag.TRUE);
        LOGGER.debug("number of all files under " + bd + " is " + filesOnlyAll.size());

        BucketDirectory bd00 = bd.appendDirectory("subdir0/subdir0");
        List<BucketPath> filesOnly00 = s.list(bd00, ListRecursiveFlag.TRUE);
        LOGGER.debug("number of files under " + bd00 + " is " + filesOnly00.size());

        s.removeBlobFolder(bd00);

        List<BucketPath> filesOnlyAllNew = s.list(bd, ListRecursiveFlag.TRUE);
        LOGGER.debug("number of all files under " + bd + " is " + filesOnlyAllNew.size());

        Assert.assertEquals(filesOnlyAllNew.size() + filesOnly00.size(), filesOnlyAll.size());

        if (s instanceof AmazonS3DFSConnection) {
            ((AmazonS3DFSConnection) s).cleanDatabase();
        }
    }

    /**
     * create a deeper structure
     */
    @Test
    public void testList8() {
        LOGGER.debug("START TEST " + new RuntimeException("").getStackTrace()[0].getMethodName());

        BucketDirectory bd = new BucketDirectory("bucket8");
        s.createContainer(bd);
        containers.add(bd);

        List<BucketPath> list = createFiles(s, bd, 3, 2, 3);
        {
            LOGGER.debug("test8 start subtest 1");
            List<BucketPath> files = s.list(bd, ListRecursiveFlag.FALSE);
            LOGGER.debug("1 einfaches listing");
            showBucketPath("list", files);
            Assert.assertEquals(2, files.size());
            compare(list,files);
        }
        {
            LOGGER.debug("test8 start subtest 2");
            List<BucketPath> files = s.list(bd, ListRecursiveFlag.TRUE);
            LOGGER.debug("2 recursives listing");
            showBucketPath("list", files);
            Assert.assertEquals(26, files.size());
            compare(list,files);
        }

        {
            LOGGER.debug("test8 start subtest 3");
            BucketDirectory bp = bd.appendDirectory("subdir1");
            List<BucketPath> files = s.list(bp, ListRecursiveFlag.FALSE);
            LOGGER.debug("3 einfaches listing");
            showBucketPath("list", files);
            Assert.assertEquals(2, files.size());
            compare(list,files);
        }

        {
            LOGGER.debug("test8 start subtest 4");
            BucketDirectory bp = bd.appendDirectory("subdir1");
            List<BucketPath> files = s.list(bp, ListRecursiveFlag.TRUE);
            LOGGER.debug("4 recursives listing");
            showBucketPath("list", files);
            Assert.assertEquals(8, files.size());
            compare(list,files);
        }

        {
            LOGGER.debug("test8 start subtest 5");
            BucketDirectory subdirectory1 = bd.appendDirectory("subdir1");
            s.removeBlobFolder(subdirectory1);
            List<BucketPath> files = s.list(bd, ListRecursiveFlag.TRUE);
            LOGGER.debug("5 recursives listing");
            showBucketPath("list", files);
            Assert.assertEquals(18, files.size());
            compare(list,files);
        }
        {
            LOGGER.debug("test8 start subtest 6");
            List<BucketPath> files = s.list(bd, ListRecursiveFlag.TRUE);
            Assert.assertFalse(s.blobExists(bd.appendDirectory("subdir1").appendName("file1")));
            Assert.assertTrue(s.blobExists(bd.appendDirectory("subdir2").appendName("file1")));
            Assert.assertFalse(s.blobExists(bd.appendDirectory("subdir2").appendName("file9")));
            compare(list,files);
        }

        {
            // Extra nicht mit expected Annotation, damit diese Exception niecht vorher schon zum
            // Abbruch anderern Tests führt
            boolean testOk = false;
            try {
                s.removeBlobFolder(bd);
            } catch (StorageConnectionException e) {
                testOk = true;
            }
            Assert.assertTrue(testOk);
        }

    }

    private void compare(List<BucketPath> list, List<BucketPath> files) {
        showBucketPath("full list", list);
        showBucketPath("found files", files);
        Assert.assertTrue(list.containsAll(files));
    }

    /**
     * same as testList6, but with different root directories for the service
     */
    @Test
    public void testList8a() {
        ConnectionProperties props = s.getConnectionProperties();
        props = changeRootDirectory(props, "deeper");
        s = DFSConnectionFactory.get(props);
        testList8();
    }
    @Test
    public void testList8b() {
        ConnectionProperties props = s.getConnectionProperties();
        props = changeRootDirectory(props, "deeper/and/deeper");
        s = DFSConnectionFactory.get(props);
        testList8();
    }


    /**
     * Überschreiben einer Datei
     */
    @Test
    public void testOverwrite() {
        BucketDirectory bd = new BucketDirectory("bucketoverwrite/1/2/3");
        s.createContainer(bd);
        containers.add(bd);

        BucketPath filea = bd.append(new BucketPath("filea"));
        Payload origPayload = new SimplePayloadImpl("1".getBytes());
        s.putBlob(filea, origPayload);
        Payload payload = s.getBlob(filea);
        Assert.assertEquals("1", new String(payload.getData()));
        LOGGER.debug("ok, inhalt nach dem ersten Schreiben ok");
        Payload newPayload = new SimplePayloadImpl("2".getBytes());
        s.putBlob(filea, newPayload);
        Assert.assertEquals("2", new String(newPayload.getData()));
        LOGGER.debug("ok, inhalt nach dem zweiten Schreiben auch ok");
    }

    @Test
    public void testListAllBuckets() {
        cleanDB();

        List<BucketDirectory> mybuckets = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            BucketDirectory bd = new BucketDirectory("bucket" + i);
            mybuckets.add(bd);
            containers.add(bd);
            s.createContainer(bd);

            Payload origPayload = new SimplePayloadImpl("1".getBytes());
            BucketPath file1 = bd.append(new BucketPath("dir1/file1"));
            s.putBlob(file1, origPayload);
            BucketPath file2 = bd.append(new BucketPath("dir1/file2"));
            s.putBlob(file2, origPayload);

        }
        List<BucketDirectory> foundBuckets = s.listAllBuckets();
        mybuckets.forEach(b -> LOGGER.debug("created bucket " + b));
        foundBuckets.forEach(b -> LOGGER.debug("found bucket " + b));
        Assert.assertTrue(foundBuckets.containsAll(mybuckets));
        Assert.assertTrue(mybuckets.containsAll(foundBuckets));
    }

    @Test
    public void testFileExists() {
        BucketDirectory bd = new BucketDirectory("bucketfileexiststest");
        s.createContainer(bd);
        containers.add(bd);

        BucketPath filea = bd.append(new BucketPath("file1"));
        Assert.assertFalse(s.blobExists(filea));
        Payload origPayload = new SimplePayloadImpl("1".getBytes());
        s.putBlob(filea, origPayload);
        Assert.assertTrue(s.blobExists(filea));
        s.removeBlob(filea);
        Assert.assertFalse(s.blobExists(filea));
    }

    @Test
    public void createBucketWithDotAndTestFileForDir() {
        LOGGER.debug("START TEST " + new RuntimeException("").getStackTrace()[0].getMethodName());
        BucketPath bucketPath = new BucketPath("user1/.hidden/Affenfile.txt");
        byte[] documentContent = "Affe".getBytes();
        s.createContainer(bucketPath.getBucketDirectory());
        s.putBlob(bucketPath, new SimplePayloadImpl(documentContent));
        BucketDirectory bd = new BucketDirectory(bucketPath);
        LOGGER.debug("bucketPath " + bucketPath);
        LOGGER.debug("pathAsDir  " + bd);
        List<BucketPath> files = s.list(bd, ListRecursiveFlag.TRUE);
        showBucketPath("list", files);
        Assert.assertTrue(files.isEmpty());
    }

    @Test
    public void destroyBucketTwice() {
        LOGGER.debug("START TEST " + new RuntimeException("").getStackTrace()[0].getMethodName());
        BucketPath bucketPath = new BucketPath("user1/.hidden/Affenfile.txt");
        byte[] documentContent = "Affe".getBytes();
        s.createContainer(bucketPath.getBucketDirectory());
        s.putBlob(bucketPath, new SimplePayloadImpl(documentContent));
        s.deleteContainer(bucketPath.getBucketDirectory());
        s.deleteContainer(bucketPath.getBucketDirectory());
    }


    @Test
    public void createManyBuckets() {
        for (int i = 0; i < 200; i++) {
            BucketDirectory bd = new BucketDirectory("bucket" + i);
            containers.add(bd);
            s.createContainer(bd);
        }
    }

    @Test
    public void saveAndReadStreamTest() {
        try {
            BucketPath bucketPath = new BucketPath("user1/.hidden/Affenfile.txt");
            byte[] content = "Affe".getBytes();
            s.createContainer(bucketPath.getBucketDirectory());
            try (ByteArrayInputStream bis = new ByteArrayInputStream(content)) {
                s.putBlobStream(bucketPath, new SimplePayloadStreamImpl(bis));
                LOGGER.debug("successfully stored stream content: " + HexUtil.convertBytesToHexString(content));
            }
            PayloadStream blobStream = s.getBlobStream(bucketPath);
            byte[] readContent = null;
            try (InputStream is = blobStream.openStream()) {
                readContent = IOUtils.toByteArray(is);
            }

            Assert.assertArrayEquals(content, readContent);
            s.deleteContainer(bucketPath.getBucketDirectory());

        } catch (Exception e) {
            throw BaseExceptionHandler.handle(e);
        }
    }
    /* =========================================================================================================== */

    private List<BucketPath> createFiles(DFSConnection DFSConnection, BucketDirectory rootDirectory, int subdirs, int subfiles, int depth) {
        List<BucketPath> list = new ArrayList<>();
        createFilesAndFoldersRecursivly(rootDirectory, subdirs, subfiles, depth, DFSConnection, list);
        return list;
    }

    private void createFilesAndFoldersRecursivly(BucketDirectory rootDirectory, int subdirs, int subfiles,
                                                 int depth, DFSConnection DFSConnection, List<BucketPath> list) {
        if (depth == 0) {
            return;
        }

        for (int i = 0; i < subfiles; i++) {
            byte[] content = ("Affe of file " + i + "").getBytes();
            BucketPath bucketPath = rootDirectory.appendName("file" + i);
            DFSConnection.putBlob(bucketPath, new SimplePayloadImpl(content));
            list.add(bucketPath);

        }
        for (int i = 0; i < subdirs; i++) {
            createFilesAndFoldersRecursivly(rootDirectory.appendDirectory("subdir" + i), subdirs, subfiles, depth - 1, DFSConnection, list);
        }
    }

    private void showBucketPath(String message, List<BucketPath> list) {
        LOGGER.debug(message);
        StringBuilder sb = new StringBuilder();
        for (BucketPath m : list) {
            LOGGER.debug(m.toString());
        }
    }

    private ConnectionProperties changeRootDirectory(ConnectionProperties props, String deeper) {
        if (props instanceof FilesystemConnectionPropertiesImpl) {
            FilesystemConnectionPropertiesImpl p = (FilesystemConnectionPropertiesImpl) props;
            String root = p.getFilesystemRootBucketName().getValue();
            root = root + BucketPath.BUCKET_SEPARATOR + deeper;
            p.setFilesystemRootBucketName(new FilesystemRootBucketName(root));
            return p;
        }

        if (props instanceof AmazonS3ConnectionProperitesImpl) {
            AmazonS3ConnectionProperitesImpl p = (AmazonS3ConnectionProperitesImpl) props;
            String root = p.getAmazonS3RootBucketName().getValue();
            root = root + BucketPath.BUCKET_SEPARATOR + deeper;
            p.setAmazonS3RootBucketName(new AmazonS3RootBucketName(root));
            return p;

        }
        throw new BaseException("unknown instance of properties:" + props);
    }


}
