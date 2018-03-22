package org.adorsys.cryptoutils.mongodbstoreconnection;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.client.gridfs.GridFSDownloadStream;
import com.mongodb.client.gridfs.GridFSFindIterable;
import com.mongodb.client.gridfs.model.GridFSDownloadOptions;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import org.adorsys.cryptoutils.exceptions.BaseException;
import org.adorsys.cryptoutils.exceptions.BaseExceptionHandler;
import org.adorsys.encobject.complextypes.BucketDirectory;
import org.adorsys.encobject.complextypes.BucketPath;
import org.adorsys.encobject.complextypes.BucketPathUtil;
import org.adorsys.encobject.domain.Payload;
import org.adorsys.encobject.domain.PayloadStream;
import org.adorsys.encobject.domain.StorageMetadata;
import org.adorsys.encobject.domain.StorageType;
import org.adorsys.encobject.exceptions.StorageConnectionException;
import org.adorsys.encobject.filesystem.StorageMetadataFlattenerGSON;
import org.adorsys.encobject.service.api.ExtendedStoreConnection;
import org.adorsys.encobject.service.impl.SimplePayloadImpl;
import org.adorsys.encobject.service.impl.SimplePayloadStreamImpl;
import org.adorsys.encobject.service.impl.SimpleStorageMetadataImpl;
import org.adorsys.encobject.service.impl.StoreConnectionListHelper;
import org.adorsys.encobject.types.ListRecursiveFlag;
import org.apache.commons.io.IOUtils;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import static com.mongodb.client.model.Filters.regex;

/**
 * Created by peter on 12.03.18 at 18:53.
 */
public class MongoDBExtendedStoreConnection implements ExtendedStoreConnection {
    private final static Logger LOGGER = LoggerFactory.getLogger(MongoDBExtendedStoreConnection.class);
    private static final String STORAGE_METADATA_KEY = "StorageMetadata";
    private static final String FILENAME_TAG = "filename";
    private static final String BUCKET_ID_FILENAME = ".bucket.creation.date.";

    private MongoDatabase database;
    private DB databaseDeprecated;
    protected StorageMetadataFlattenerGSON gsonHelper = new StorageMetadataFlattenerGSON();


    public MongoDBExtendedStoreConnection(String databasename) {
        MongoClient mongoClient = new MongoClient();
        database = mongoClient.getDatabase(databasename);
        databaseDeprecated = mongoClient.getDB(databasename);
    }


    public MongoDBExtendedStoreConnection() {
        this("default-database");
    }

    @Override
    public void putBlob(BucketPath bucketPath, Payload payload) {
        putBlobStream(bucketPath, new SimplePayloadStreamImpl(payload.getStorageMetadata(), new ByteArrayInputStream(payload.getData())));
    }

    @Override
    public Payload getBlob(BucketPath bucketPath) {
        try {
            PayloadStream blobStream = getBlobStream(bucketPath);
            return new SimplePayloadImpl(blobStream.getStorageMetadata(), IOUtils.toByteArray(blobStream.openStream()));
        } catch (Exception e) {
            throw BaseExceptionHandler.handle(e);
        }
    }

    @Override
    public void putBlobStream(BucketPath bucketPath, PayloadStream payloadStream) {
        LOGGER.info("start putBlobStream for " + bucketPath);
        GridFSBucket bucket = getGridFSBucket(bucketPath);
        checkBucketExists(bucket);
        String filename = bucketPath.getObjectHandle().getName();

        GridFSUploadOptions uploadOptions = new GridFSUploadOptions();
        uploadOptions.metadata(new Document());
        SimpleStorageMetadataImpl storareMetaData = new SimpleStorageMetadataImpl(payloadStream.getStorageMetadata());
        storareMetaData.setType(StorageType.BLOB);
        storareMetaData.setName(BucketPathUtil.getAsString(bucketPath));
        uploadOptions.getMetadata().put(STORAGE_METADATA_KEY, gsonHelper.toJson(storareMetaData));
        InputStream is = payloadStream.openStream();
        ObjectId objectId = bucket.uploadFromStream(filename, is, uploadOptions);
        IOUtils.closeQuietly(is);
        deleteAllExcept(bucket, filename, objectId);

        LOGGER.info("finished putBlobStream for " + bucketPath);
    }

    @Override
    public PayloadStream getBlobStream(BucketPath bucketPath) {
        LOGGER.info("start getBlobStream for " + bucketPath);
        GridFSBucket bucket = getGridFSBucket(bucketPath);
        checkBucketExists(bucket);
        String filename = bucketPath.getObjectHandle().getName();

        GridFSDownloadOptions options = new GridFSDownloadOptions();
        GridFSDownloadStream fileStream = bucket.openDownloadStream(filename, options);
        PayloadStream payloadStream = new SimplePayloadStreamImpl(getStorageMetadata(bucketPath), fileStream);
        LOGGER.info("finished getBlobStream for " + bucketPath);
        return payloadStream;
    }

    @Override
    public void putBlob(BucketPath bucketPath, byte[] bytes) {
        putBlob(bucketPath, new SimplePayloadImpl(new SimpleStorageMetadataImpl(), bytes));
    }

    @Override
    public StorageMetadata getStorageMetadata(BucketPath bucketPath) {
        GridFSBucket bucket = getGridFSBucket(bucketPath);
        checkBucketExists(bucket);
        GridFS gridFS = new GridFS(databaseDeprecated, bucketPath.getObjectHandle().getContainer());
        GridFSDBFile one = gridFS.findOne(bucketPath.getObjectHandle().getName());
        String jsonString = (String) one.getMetaData().get(STORAGE_METADATA_KEY);
        return gsonHelper.fromJson(jsonString);
    }

    @Override
    public boolean blobExists(BucketPath bucketPath) {
        LOGGER.info("start blob Exists for " + bucketPath);
        GridFSBucket bucket = getGridFSBucket(bucketPath);
        checkBucketExists(bucket);
        String filename = bucketPath.getObjectHandle().getName();
        List<ObjectId> ids = new ArrayList<>();
        bucket.find(Filters.eq(FILENAME_TAG, filename)).forEach((Consumer<GridFSFile>) file -> ids.add(file.getObjectId()));
        LOGGER.info("finished blob Exists for " + bucketPath);
        return !ids.isEmpty();
    }

    @Override
    public void removeBlob(BucketPath bucketPath) {
        LOGGER.info("start removeBlob for " + bucketPath);
        GridFSBucket bucket = getGridFSBucket(bucketPath);
        checkBucketExists(bucket);
        String filename = bucketPath.getObjectHandle().getName();
        List<ObjectId> ids = new ArrayList<>();
        bucket.find(Filters.eq(FILENAME_TAG, filename)).forEach((Consumer<GridFSFile>) file -> ids.add(file.getObjectId()));
        ids.forEach(id -> bucket.delete(id));
        LOGGER.info("finished removeBlob for " + bucketPath);
    }

    @Override
    public void removeBlobFolder(BucketDirectory bucketDirectory) {
        LOGGER.info("start removeBlobFolder for " + bucketDirectory);
        if (bucketDirectory.getObjectHandle().getName() == null) {
            throw new StorageConnectionException("not a valid bucket directory " + bucketDirectory);
        }
        GridFSBucket bucket = getGridFSBucket(bucketDirectory);
        String directoryname = bucketDirectory.getObjectHandle().getName() + BucketPath.BUCKET_SEPARATOR;
        String pattern = "^" + directoryname + ".*";
        GridFSFindIterable list = bucket.find(regex(FILENAME_TAG, pattern, "i"));
        list.forEach((Consumer<GridFSFile>) file -> bucket.delete(file.getObjectId()));
        LOGGER.info("finished removeBlobFolder for " + bucketDirectory);
    }

    @Override
    public void removeBlobs(Iterable<BucketPath> bucketPaths) {
        bucketPaths.forEach(bucketPath -> removeBlob(bucketPath));
    }

    @Override
    public long countBlobs(BucketDirectory bucketDirectory, ListRecursiveFlag recursive) {
        return list(bucketDirectory, recursive).size();
    }

    @Override
    public void createContainer(BucketDirectory bucketDirectory) {
        GridFSBucket bucket = GridFSBuckets.create(database, bucketDirectory.getObjectHandle().getContainer());
        InputStream is = new ByteArrayInputStream(new Date().toString().getBytes());
        bucket.uploadFromStream(BUCKET_ID_FILENAME, is);
        IOUtils.closeQuietly(is);
    }

    @Override
    public boolean containerExists(BucketDirectory bucketDirectory) {
        GridFSBucket bucket = GridFSBuckets.create(database, bucketDirectory.getObjectHandle().getContainer());
        return containerExists(bucket);
    }

    @Override
    public void deleteContainer(BucketDirectory bucketDirectory) {
        BucketPathUtil.checkContainerName(bucketDirectory.getObjectHandle().getContainer());
        GridFSBuckets.create(database, bucketDirectory.getObjectHandle().getContainer()).drop();

    }

    @Override
    public List<StorageMetadata> list(BucketDirectory bucketDirectory, ListRecursiveFlag listRecursiveFlag) {
        LOGGER.info("start list for " + bucketDirectory);
        GridFSBucket bucket = getGridFSBucket(bucketDirectory);
        List<StorageMetadata> list = new ArrayList<>();
        if (!containerExists(bucket)) {
            LOGGER.debug("container " + bucket.getBucketName() + " existiert nicht, daher leere Liste");
            return list;
        }

        if (bucketDirectory.getObjectHandle().getName() != null) {
            if (bucket.find(Filters.eq(FILENAME_TAG, bucketDirectory.getObjectHandle().getName())).iterator().hasNext()) {
                // Spezialfall, das übergebene Directory ist eine Datei. In diesem Fall geben wir
                // eine leere Liste zurück
                return list;
            }
        }


        String directoryname = (bucketDirectory.getObjectHandle().getName() != null)
                ? bucketDirectory.getObjectHandle().getName() + BucketPath.BUCKET_SEPARATOR
                : "";

        List<BucketPath> bucketPaths = new ArrayList<>();
        Set<BucketDirectory> dirs = new HashSet<>();
        if (listRecursiveFlag.equals(ListRecursiveFlag.TRUE)) {
            String pattern = "^" + directoryname + ".*";
            GridFSFindIterable gridFSFiles = bucket.find(regex(FILENAME_TAG, pattern, "i"));
            gridFSFiles.forEach((Consumer<GridFSFile>) file -> bucketPaths.add(
                    new BucketPath(bucketDirectory.getObjectHandle().getContainer(), file.getFilename())));
            LOGGER.debug("found recursive " + bucketPaths.size());
            dirs.addAll(StoreConnectionListHelper.findAllSubDirs(bucketPaths));
        } else {
            // files only
            String pattern = "^" + directoryname + "[^/]*$";
            GridFSFindIterable gridFSFiles = bucket.find(regex(FILENAME_TAG, pattern, "i"));
            gridFSFiles.forEach((Consumer<GridFSFile>) file -> bucketPaths.add(
                    new BucketPath(bucketDirectory.getObjectHandle().getContainer(), file.getFilename())));
            LOGGER.debug("found non-recursive " + bucketPaths.size());

            dirs.addAll(findSubdirs(bucket, bucketDirectory));
        }

        bucketPaths.forEach(bucketPath -> {
            if (!bucketPath.getObjectHandle().getName().equals(BUCKET_ID_FILENAME)) {
                list.add(getStorageMetadata(bucketPath));
            }
        });

        dirs.add(bucketDirectory);
        dirs.forEach(dir -> {
            SimpleStorageMetadataImpl storageMetadata = new SimpleStorageMetadataImpl();
            storageMetadata.setType(StorageType.FOLDER);
            storageMetadata.setName(BucketPathUtil.getAsString(dir));
            list.add(storageMetadata);
        });

        LOGGER.info("list(" + bucketDirectory + ")");
        list.forEach(c -> LOGGER.debug(" > " + c.getName() + " " + c.getType()));
        return list;
    }

    @Override
    public List<BucketDirectory> listAllBuckets() {
        List<BucketDirectory> list = new ArrayList<>();
        databaseDeprecated.getCollectionNames().forEach(el -> {
            if (el.endsWith(".files")) {
                String collectionName= el.substring(0, el.length() - ".files".length());
                list.add(new BucketDirectory(collectionName));
            }
        });
        return list;
    }


    // =========================================================================================


    private GridFSBucket getGridFSBucket(BucketPath bucketPath) {
        return GridFSBuckets.create(database, bucketPath.getObjectHandle().getContainer());
    }

    private GridFSBucket getGridFSBucket(BucketDirectory bucketDirectory) {
        return GridFSBuckets.create(database, bucketDirectory.getObjectHandle().getContainer());
    }


    private void deleteAllExcept(GridFSBucket bucket, String filename, ObjectId objectID) {
        List<ObjectId> idsToDelete = new ArrayList<>();
        bucket.find(Filters.eq(FILENAME_TAG, filename)).forEach((Consumer<GridFSFile>) file -> idsToDelete.add(file.getObjectId()));
        LOGGER.info("****  number of files to delete:" + idsToDelete.size());
        idsToDelete.forEach(id -> {
            if (!id.equals(objectID)) {
                LOGGER.info("****  delete:" + id);
                bucket.delete(id);
            }
        });
    }

    private Set<BucketDirectory> findSubdirs(GridFSBucket bucket, BucketDirectory bucketDirectory) {
        String prefix = (bucketDirectory.getObjectHandle().getName() != null)
                ? bucketDirectory.getObjectHandle().getName() + BucketPath.BUCKET_SEPARATOR
                : "";
        List<String> allFiles = new ArrayList<>();
        {
            // all files
            String pattern = "^" + prefix + ".*";
            GridFSFindIterable gridFSFiles = bucket.find(regex(FILENAME_TAG, pattern, "i"));
            gridFSFiles.forEach((Consumer<GridFSFile>) file -> allFiles.add(file.getFilename()));
        }
        Set<BucketDirectory> dirsOnly = new HashSet<>();
        allFiles.forEach(filename -> {
            if (filename.length() < prefix.length()) {
                // Absoluter Sonderfall. Z.B. es exisitiert a/b.txt und gesucht wurde mit a/b.txt
            } else {
                String remainder = filename.substring(prefix.length());
                int pos = remainder.indexOf(BucketPath.BUCKET_SEPARATOR);
                if (pos != -1) {
                    String dirname = remainder.substring(0, pos);
                    dirsOnly.add(bucketDirectory.appendDirectory(dirname));
                }
            }
        });
        return dirsOnly;
    }

    /*
    private Set<BucketDirectory> findAllSubDirs(List<String> filenames, BucketDirectory bucketDirectory) {
        Set<String> allDirs = new HashSet<>();
        filenames.forEach(filename -> {
            int last = filename.lastIndexOf(BucketPath.BUCKET_SEPARATOR);
            if (last != -1) {
                allDirs.add(filename.substring(0, last));
            }
        });
        Set<BucketDirectory> list = new HashSet<>();
        allDirs.forEach(dir -> {
            list.add(new BucketDirectory(bucketDirectory.getObjectHandle().getContainer() + BucketPath.BUCKET_SEPARATOR + dir));
        });
        return list;
    }
    */

    private boolean containerExists(GridFSBucket bucket) {
        return (bucket.find(Filters.eq(FILENAME_TAG, BUCKET_ID_FILENAME)).iterator().hasNext());
    }

    private void checkBucketExists(GridFSBucket bucket) {
        if (!containerExists(bucket)) {
            throw new BaseException("Container " + bucket.getBucketName() + " does not exist yet");
        }
    }

}
