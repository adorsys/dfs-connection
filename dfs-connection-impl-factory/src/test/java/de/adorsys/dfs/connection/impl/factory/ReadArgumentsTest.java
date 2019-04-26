package de.adorsys.dfs.connection.impl.factory;

import de.adorsys.dfs.connection.api.types.properties.AmazonS3ConnectionProperties;
import de.adorsys.dfs.connection.api.types.properties.ConnectionProperties;
import de.adorsys.dfs.connection.api.types.properties.FilesystemConnectionProperties;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by peter on 13.04.18 at 19:30.
 */
public class ReadArgumentsTest {
    private final static Logger LOGGER = LoggerFactory.getLogger(ReadArgumentsTest.class);
    String minio;
    String mongo;
    String amazon;
    String filesys;

    @Before
    public void before() {
        minio = System.getProperty(ReadArguments.AMAZONS3);
        filesys = System.getProperty(ReadArguments.FILESYSTEM);
        System.clearProperty(ReadArguments.AMAZONS3);
        System.clearProperty(ReadArguments.FILESYSTEM);
    }

    @After
    public void after() {
        LOGGER.debug("----------------");
        System.clearProperty(ReadArguments.AMAZONS3);
        System.clearProperty(ReadArguments.FILESYSTEM);
    }

    @Test
    public void testEnvFilesystem1() {
        System.setProperty(ReadArguments.FILESYSTEM, "target/filesystem");
        ConnectionProperties properties = new ReadArguments().readEnvironment();
        Assert.assertTrue(properties instanceof FilesystemConnectionProperties);
    }

    @Test
    public void testEnvFilesystem2() {
        System.setProperty(ReadArguments.FILESYSTEM, "");
        ConnectionProperties properties = new ReadArguments().readEnvironment();
        Assert.assertTrue(properties instanceof  FilesystemConnectionProperties);
    }

    @Test
    public void testArgFilesystem1() {
        String[] args = new String[1];
        args[0] = ReadArguments.FILESYSTEM_ARG + "target/filesystem";
        ReadArguments.ArgsAndProperties argsAndProperties = new ReadArguments().readArguments(args);
        Assert.assertEquals(0, argsAndProperties.remainingArgs.length);
    }

    @Test
    public void tesArgFilesystem2() {
        String[] args = new String[1];
        args[0] = ReadArguments.FILESYSTEM_ARG;
        ReadArguments.ArgsAndProperties argsAndProperties = new ReadArguments().readArguments(args);
        Assert.assertEquals(0, argsAndProperties.remainingArgs.length);
    }

    @Test
    public void testParam3Args() {
        String[] args = new String[2];
        args[0] = ReadArguments.AMAZONS3_ARG + "http:1,key,key";
        args[1] = "anyParam";
        ReadArguments.ArgsAndProperties argsAndProperties = new ReadArguments().readArguments(args);
        Assert.assertEquals(1, argsAndProperties.remainingArgs.length);
    }


    @Test
    public void testEnv3Args() {
        System.setProperty(ReadArguments.AMAZONS3,"http:1,key,key");
        System.setProperty("any","any");
        ConnectionProperties properties = new ReadArguments().readEnvironment();
        Assert.assertTrue(properties instanceof AmazonS3ConnectionProperties);
    }

}
