package org.homer.versioner.sql.exceptions;

public class DatabasePersistenceException extends RuntimeException{
    public DatabasePersistenceException(String s) {
        super(s);
    }
}
