package org.adorsys.dfs.connection.impl.mongodb;

import org.adorsys.dfs.connection.api.types.connection.MongoURI;
import org.adorsys.dfs.connection.api.types.properties.ConnectionPropertiesImpl;
import org.adorsys.dfs.connection.api.types.properties.MongoConnectionProperties;
/**
 * Created by peter on 04.10.18.
 */
public class MongoConnectionPropertiesImpl extends ConnectionPropertiesImpl implements MongoConnectionProperties {
    private MongoURI mongoURI = null;

    public MongoConnectionPropertiesImpl() {}

    public MongoConnectionPropertiesImpl(MongoConnectionProperties source) {
        super(source);
        mongoURI = source.getMongoURI();
    }

    @Override
    public MongoURI getMongoURI() {
        return mongoURI;
    }

    public void setMongoURI(MongoURI mongoURI) {
        this.mongoURI = mongoURI;
    }
}
