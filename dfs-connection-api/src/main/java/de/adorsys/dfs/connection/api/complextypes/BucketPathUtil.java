package de.adorsys.dfs.connection.api.complextypes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

/**
 * Created by peter on 20.02.18 at 08:37.
 */
public class BucketPathUtil {
    private final static String WINDOWS_BUCKET_PATH_SEPARATOR = "\\\\";
    private final static Logger LOGGER = LoggerFactory.getLogger(BucketPathUtil.class);

    /**
     * Separiert alle Elemente. Doppelte Slashes werden ignoriert.
     */
    public static List<String> split(String fullBucketPath) {
        List<String> list = new ArrayList<>();
        if (fullBucketPath == null) {
            return list;
        }
        fullBucketPath = fullBucketPath.replaceAll(WINDOWS_BUCKET_PATH_SEPARATOR, BucketPath.BUCKET_SEPARATOR);
        StringTokenizer st = new StringTokenizer(fullBucketPath, BucketPath.BUCKET_SEPARATOR);
        while (st.hasMoreElements()) {
            String token = st.nextToken();
            if (notOnlyWhitespace(token)) {
                list.add(token);
            }
        }
        return list;
    }

    private static boolean notOnlyWhitespace(String value) {
        return value.replaceAll(" ", "").length() > 0;
    }
}
