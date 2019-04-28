# DFS Connection

The DFS Connecton is a layer to use file based Districbuted File System (DFS) storage systems. The interface has methods to read, write, update and delete blobs. Currently two implementations of the interface are provided:

0. filesystem
0. amazons3

The filesystem implementation stores every blob on the filesystem, starting in the rootdirectory, provided to the implementation. If you change code, make sure, it is running on windows platforms too.

The amazons3 implementation stores every blob on the url provided, starting in the rootbucket, which is provided too. As the interface is pretty easy and does not contain versioning, it is supported for 
 0. amazons3
 0. minio
 0. ceph
 
# Hello world

The DFS Connection is to be linked and used directly into the code. There is no spring injection.
To retrieve a DFS Connection use
```
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
``` 

As can be seen the tricky part is the usae of the DFSConnectionFactory. If no parameter is provided, the environment is read. If nothing can be read a "default filesystem dfs" is used. Otherwise the one provided. 

### To use the filesystem explicitly use:
```
-DSC-FILESYSTEM=/tmp
```
in this case, the root directory is an absolute path, because it starts with a slash.
To use a relative path, omit the slash. 

### To use the amazons3 implementation use: 
```
-DSC-AMAZONS3=<url>,<accesskex>,<secretkey>,<region>,<root bucket>
```

actually for minio and ceph, the first three params a sufficiant, for example for a local minio:
```
-DSC-AMAZONS3=http://192.168.178.60:9000,simpleAccessKey,simpleSecretKey
```

Of course the url provided must be supported by a running server like amamzon aws, minio or ceph system.


 
# others 
 * [how to create a release](.docs/HowToCreateARelease.md)
