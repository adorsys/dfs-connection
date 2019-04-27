package de.adorsys.dfs.connection.api.complextypes;

import de.adorsys.common.exceptions.BaseException;
import de.adorsys.dfs.connection.api.types.BucketName;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Created by peter on 29.01.18 at 14:40.
 */
public class BucketDirectory {
    private String name = null;

    public BucketDirectory(String path) {
        List<String> split = BucketPathUtil.split(path);
        if (!split.isEmpty()) {
            if (!split.isEmpty()) {
                name = split.stream().map(b -> b).collect(Collectors.joining(BucketName.BUCKET_SEPARATOR));
            }
        } else {
            name = "/";
        }
    }

    public BucketDirectory(BucketPath bucketPath) {
        this.name = bucketPath.name;
    }

    public BucketDirectory append(BucketDirectory directory) {
        return new BucketDirectory(name + BucketPath.BUCKET_SEPARATOR + directory.name);
    }

    public BucketPath append(BucketPath bucketPath) {
        return new BucketPath(name + BucketPath.BUCKET_SEPARATOR + bucketPath.name);
    }

    public BucketDirectory appendDirectory(String directory) {
        return new BucketDirectory(name + BucketPath.BUCKET_SEPARATOR + directory);
    }

    public BucketPath appendName(String name) {
        return append(new BucketPath(name));
    }

    public String getValue() {
        return name;
    }

    public String getContainer() {
        if (name.length() == 1) {
            throw new BaseException("container can not be " + name);
        }
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
        return "BucketDirectory{" + name + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BucketDirectory that = (BucketDirectory) o;
        return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }
}
