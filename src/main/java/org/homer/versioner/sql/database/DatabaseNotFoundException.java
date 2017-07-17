package org.homer.versioner.sql.database;

public class DatabaseNotFoundException extends RuntimeException {
    public DatabaseNotFoundException() {
        super ("The given database is not supported yet.");
    }
}
