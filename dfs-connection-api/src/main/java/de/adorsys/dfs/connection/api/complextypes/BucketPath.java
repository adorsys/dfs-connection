package de.adorsys.dfs.connection.api.complextypes;

import de.adorsys.common.exceptions.BaseException;
import de.adorsys.dfs.connection.api.types.BucketName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Created by peter on 16.01.18.
 * Die einzige Aufgabe des BucketPath ist es, für die LowLevel Speicheroperationen einen
 * ObjectHandle zur Verfügung zu stellen. Dieser wiederum besteht aus einem Container und
 * einem Namen. Dabei kann der Name auch Verzeichnisse haben. Auch kann es sein, dass es
 * keinen Container gibt!
 */
public class BucketPath {
    public final static String BUCKET_SEPARATOR = "/";
    private final static Logger LOGGER = LoggerFactory.getLogger(BucketPath.class);

    String name = null;

    /**
     * Wenn path einen Slash enthält, dann ist der Teil vor dem ersten Slash der Container und der Rest der Name
     * Wenn path keinen Slash enthält, dann ist alles der Container und der Name leer
     */
    public BucketPath(String path) {
        List<String> split = BucketPathUtil.split(path);
        if (!split.isEmpty()) {
            if (!split.isEmpty()) {
                name = split.stream().map(b -> b).collect(Collectors.joining(BucketName.BUCKET_SEPARATOR));
            }
        } else {
            throw new BaseException("BucketPatb must not be empty:" + path);
        }
    }

    public BucketPath(BucketPath bucketPath) {
        this.name = bucketPath.name;
    }

    /**
     * @return returns the new concatenated bucketPath
     * the BucketPath itself keeps untuched
     */
    public BucketPath append(BucketPath bucketPath) {
        return new BucketPath(name + BucketPath.BUCKET_SEPARATOR + bucketPath.name);
    }

    public BucketPath append(String path) {
        return append(new BucketPath(path));
    }

    public BucketPath add(String suffix) {
        return new BucketPath(name + suffix);
    }

    public String getValue() {
        return name;
    }

    public String getContainer() {
        return BucketPathUtil.split(name).get(0);
    }

    public String getName() {
        List<String> split = BucketPathUtil.split(name);
        split.remove(0);
        String result;
        if (!split.isEmpty()) {
            if (!split.isEmpty()) {
                return split.stream().map(b -> b).collect(Collectors.joining(BucketName.BUCKET_SEPARATOR));
            }
        }
        return "";
    }


    @Override
    public String toString() {
        return "BucketPath{" + name + '}';
    }

    public BucketDirectory getBucketDirectory() {
        int index = name.lastIndexOf(BUCKET_SEPARATOR);
        if (index == -1) {
            return new BucketDirectory("/");
        }
        return new BucketDirectory(name.substring(0, index));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BucketPath that = (BucketPath) o;
        return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }
}
