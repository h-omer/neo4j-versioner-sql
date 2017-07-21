package org.homer.versioner.sql.exception;

import java.sql.SQLException;

public class ConnectionException extends RuntimeException {
    public ConnectionException(SQLException e){
        super(e);
    }
}
