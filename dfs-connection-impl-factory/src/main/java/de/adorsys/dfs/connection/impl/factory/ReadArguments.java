package de.adorsys.dfs.connection.impl.factory;

import de.adorsys.common.exceptions.BaseExceptionHandler;
import de.adorsys.dfs.connection.api.filesystem.FileSystemParamParser;
import de.adorsys.dfs.connection.api.types.properties.ConnectionProperties;
import de.adorsys.dfs.connection.impl.amazons3.AmazonS3ParamParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by peter on 27.03.18 at 20:20.
 */
public class ReadArguments {
    private final static Logger LOGGER = LoggerFactory.getLogger(ReadArguments.class);
    private static final String SYSTEM_PROPERTY_PREFIX = "-D";
    public static final String AMAZONS3 = "SC-AMAZONS3";
    public static final String FILESYSTEM = "SC-FILESYSTEM";


    public static final String AMAZONS3_ARG = SYSTEM_PROPERTY_PREFIX + AMAZONS3 + "=";
    public static final String FILESYSTEM_ARG = SYSTEM_PROPERTY_PREFIX + FILESYSTEM + "=";

    public ArgsAndProperties readArguments(String[] args) {
        Arrays.stream(args).forEach(arg -> LOGGER.debug("readArguments arg:" + arg));

        List<String> remainingArgs = new ArrayList<>();
        ConnectionProperties properties = null;
        for (String arg : args) {
            if (arg.startsWith(AMAZONS3_ARG)) {
                properties = AmazonS3ParamParser.getProperties(arg.substring(AMAZONS3_ARG.length()));
            } else if (arg.startsWith(FILESYSTEM_ARG)) {
                properties = FileSystemParamParser.getProperties(arg.substring(FILESYSTEM_ARG.length()));
            } else {
                remainingArgs.add(arg);
            }
        }

        if (properties == null) {
            properties = FileSystemParamParser.getProperties("");
        }

        String[] remainingArgArray = new String[remainingArgs.size()];
        remainingArgArray = remainingArgs.toArray(remainingArgArray);
        return new ArgsAndProperties(properties, remainingArgArray);
    }

    public ConnectionProperties readEnvironment() {
        try {
            LOGGER.debug("readEnvironment");
            ConnectionProperties properties = null;

            if (System.getProperty(AMAZONS3) != null) {
                properties = AmazonS3ParamParser.getProperties(System.getProperty(AMAZONS3));
            }
            if (System.getProperty(FILESYSTEM) != null) {
                properties = FileSystemParamParser.getProperties(System.getProperty(FILESYSTEM));
            }

            if (properties == null) {
                properties = FileSystemParamParser.getProperties("");
            }

            return properties;
        } catch (Exception e) {
            throw BaseExceptionHandler.handle(e);
        }
    }


    public static class ArgsAndProperties {
        public ConnectionProperties properties;
        public String[] remainingArgs;

        public ArgsAndProperties(ConnectionProperties properties, String[] remainingArgs) {
            this.properties = properties;
            this.remainingArgs = remainingArgs;
        }
    }
}
