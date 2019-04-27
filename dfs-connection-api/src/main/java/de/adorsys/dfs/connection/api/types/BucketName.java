package de.adorsys.dfs.connection.api.types;

import de.adorsys.common.basetypes.BaseTypeString;
import de.adorsys.dfs.connection.api.complextypes.BucketPath;
import de.adorsys.dfs.connection.api.exceptions.InvalidBucketNameException;

/**
 * Created by peter on 16.01.18.
 */
public class BucketName extends BaseTypeString {
    public BucketName() {}

    public BucketName(String value) {
        super(value);
        if (value.indexOf(BucketPath.BUCKET_SEPARATOR) != -1) {
            throw new InvalidBucketNameException("BucketName " + value + " must not contain " + BucketPath.BUCKET_SEPARATOR);
        }
    }
}
