package org.homer.versioner.sql.exceptions;

import java.sql.SQLException;

public class SQLVersionerException extends RuntimeException {

    public SQLVersionerException(String s) {
        super(s);
    }
}
