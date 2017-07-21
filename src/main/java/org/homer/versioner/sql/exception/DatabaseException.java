package org.homer.versioner.sql.exception;

import java.sql.SQLException;

public class DatabaseException extends RuntimeException {
    public DatabaseException(SQLException e) {
        super(e);
    }
}
