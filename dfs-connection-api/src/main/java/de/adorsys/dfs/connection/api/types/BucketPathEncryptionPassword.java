package de.adorsys.dfs.connection.api.types;

import de.adorsys.common.basetypes.BaseTypePasswordString;
import de.adorsys.common.exceptions.BaseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by peter on 26.09.18.
 */
public class BucketPathEncryptionPassword extends BaseTypePasswordString {
    private final static Logger LOGGER = LoggerFactory.getLogger(BucketPathEncryptionPassword.class);
    public final static String specialChars = " <>{}^!\"§$%&/()=?´`{}#'*";
    public BucketPathEncryptionPassword(String s) {
        super(s);
        if (s.equalsIgnoreCase("null")) {
            throw new BaseException("Password must not be \"null\". Null should set the password to NULL");
        }
        /*
        DOC-76
        if (s.length() < 10) {
            throw new BaseException("Password is not complex enough. At least 10 characters long");
        }
        if (s.toLowerCase().equals(s)) {
            throw new BaseException("Password is not complex enough. At least one uppercase char");
        }
        if (s.toUpperCase().equals(s)) {
            throw new BaseException("Password is not complex enough. At least one lowercase char");
        }
        if (!s.matches(".*[01234567890].*")) {
            throw new BaseException("Password is not complex enough. At least one digit");
        }
        if (!s.matches(".*["+specialChars+"]{1}.*")) {
            throw new BaseException("Password is not complex enough. At least one special char of \""+specialChars+"\"");
        }
        */

    }
}
