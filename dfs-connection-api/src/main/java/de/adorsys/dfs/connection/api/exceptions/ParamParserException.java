package de.adorsys.dfs.connection.api.exceptions;


import de.adorsys.common.exceptions.BaseException;

/**
 * Created by peter on 18.06.18 at 22:46.
 */
public class ParamParserException extends BaseException {
    public ParamParserException(String params, String delimter, String expectedParams) {
        super("can not parse >" + params + "< with >" + delimter + "< as delimiter. Expected params: " + expectedParams);
    }
}
