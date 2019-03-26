package org.adorsys.dfs.connection.impl.mongodb;


import org.adorsys.dfs.connection.api.exceptions.ParamParserException;
import org.adorsys.dfs.connection.api.types.connection.MongoURI;
import org.adorsys.dfs.connection.api.types.properties.MongoConnectionProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by peter on 13.04.18 at 16:19.
 */
public class MongoParamParser {
    private final static Logger LOGGER = LoggerFactory.getLogger(MongoParamParser.class);
    private final static String DELIMITER = ",";
    public final static String EXPECTED_PARAMS = "<mongoClientUri> (mongodb://[username:password@]host1[:port1][,host2[:port2],...[,hostN[:portN]]][/[database.collection][?options]]) - see http://mongodb.github.io/mongo-java-driver/3.6/javadoc/com/mongodb/MongoClientURI.html";

    public static MongoConnectionProperties getProperties(String params) {
        LOGGER.debug("parse:" + params);
        try {
            MongoConnectionPropertiesImpl props = new MongoConnectionPropertiesImpl();

            if (params.length() > 0) {
                props.setMongoURI(new MongoURI(params));
            }
            return props;
        } catch (Exception e) {
            throw new ParamParserException(params, DELIMITER, EXPECTED_PARAMS);
        }
    }
}
