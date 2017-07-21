package org.homer.versioner.sql.exception;

public class DatabaseNotFoundException extends RuntimeException {
    public DatabaseNotFoundException(String dbName) {
        super (String.format("Database not supported (%s)", dbName));
    }
}
