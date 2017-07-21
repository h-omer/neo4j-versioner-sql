package org.homer.versioner.sql.exceptions;

public class DatabaseNotFoundException extends RuntimeException {
    public DatabaseNotFoundException(String dbName) {
        super (String.format("Database not supported (%s)", dbName));
    }
}
