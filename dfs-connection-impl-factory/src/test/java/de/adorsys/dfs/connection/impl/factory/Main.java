package de.adorsys.dfs.connection.impl.factory;

import de.adorsys.dfs.connection.api.complextypes.BucketPath;
import de.adorsys.dfs.connection.api.domain.Payload;
import de.adorsys.dfs.connection.api.service.api.DFSConnection;
import de.adorsys.dfs.connection.api.service.impl.SimplePayloadImpl;
import org.junit.Assert;

public class Main {
    public static void main(String[] args) {
        System.out.println("create a payload, which is no more than a byte array");
        Payload payload = new SimplePayloadImpl("the content of the file".getBytes());

        System.out.println("create a path, where the data is to be stored");
        BucketPath bucketPath = new BucketPath("/a/path/to/a/file.txt");

        System.out.println("the tricky part, get a connection. Default is a filesystem connection starting in ./target/filesystemstorage");
        DFSConnection dfsConnection = DFSConnectionFactory.get();

        System.out.println("check if document exists");
        if (dfsConnection.blobExists(bucketPath)) {
            System.out.println(bucketPath.getValue() + " already exists, so it will be overwritten");
        }

        System.out.println("storing the data");
        dfsConnection.putBlob(bucketPath, payload);

        System.out.println("rereading the date");
        Payload readPayload = dfsConnection.getBlob(bucketPath);

        System.out.println("proof that the data has not been corrupted");
        Assert.assertArrayEquals(payload.getData(), readPayload.getData());
    }
}
