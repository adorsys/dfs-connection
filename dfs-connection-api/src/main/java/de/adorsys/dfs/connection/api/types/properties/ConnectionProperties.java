package de.adorsys.dfs.connection.api.types.properties;

import de.adorsys.dfs.connection.api.types.BucketPathEncryptionPassword;

/**
 * Created by peter on 04.10.18.
 */
public interface ConnectionProperties {
    BucketPathEncryptionPassword defaultEncryptionPassword = new BucketPathEncryptionPassword("2837/(&dfja34j39,yiEsdkfhasDfkljh");
    BucketPathEncryptionFilenameOnly defaultBucketPathEncryptionFilenameOnly = BucketPathEncryptionFilenameOnly.FALSE;


    BucketPathEncryptionPassword getBucketPathEncryptionPassword();

    BucketPathEncryptionFilenameOnly getBucketPathEncryptionFilenameOnly();
}
