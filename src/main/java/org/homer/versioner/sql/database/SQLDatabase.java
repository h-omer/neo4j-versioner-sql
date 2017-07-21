package org.homer.versioner.sql.database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public abstract class SQLDatabase {

    /**
     * It returns the specific database name
     *
     * @return specific database name
     */
    public abstract String getName();

    /**
     * It returns the specific Connection URL for JDBC Driver
     *
     * @return specific connection URL String
     */
    public Connection getConnection(String hostName, Long port, String databaseName, String username, String password) throws SQLException {
        String connectionUrl = String.format(getConnectionUrl(), hostName, port, databaseName);
        return DriverManager.getConnection(connectionUrl, username, password);
    }

    /**
     * It returns the specific query {@link String} for schema information
     *
     * @return specific schema query String
     */
    public abstract String getSchemaQuery();

    /**
     * It returns the specific query {@link String} for tables information
     *
     * @param schema specific schema
     * @return specific tables query String
     */
    public String buildTablesQuery(String schema) {
        return String.format(getTablesQuery(), schema);
    }

    /**
     * It returns the specific query {@link String} for column information
     *
     * @param schema specific schema
     * @param table specific table
     * @return specific columns query String
     */
    public String buildColumnsQuery(String schema, String table){
        return String.format(getColumnsQuery(), schema, table);
    }

    /**
     * It returns the specific query {@link String} for foreign keys information
     *
     * @return specific foreign keys query String
     */
    public abstract String getForeignKeysQuery();


    protected abstract String getConnectionUrl();

    protected abstract String getTablesQuery();

    protected abstract String getColumnsQuery();
}
