package org.homer.versioner.sql.database;

public interface SQLDatabase {

    /**
     * It returns the specific database brand name
     *
     * @return specific database brand name
     */
    String getDatabaseBrand();

    /**
     * It returns the specific Connection URL for JDBC Driver
     *
     * @return specific connection URL String
     */
    String getConnectionURL(String hostName, Long port, String databaseName);

    /**
     * It returns the specific query {@link String} for schema information
     *
     * @return specific schema query String
     */
    String getSchemaQuery();

    /**
     * It returns the specific query {@link String} for tables information
     *
     * @param schema specific schema
     * @return specific tables query String
     */
    String getTablesQuery(String schema);

    /**
     * It returns the specific query {@link String} for column information
     *
     * @param schema specific schema
     * @param table specific table
     * @return specific columns query String
     */
    String getColumnsQuery(String schema, String table);

    /**
     * It returns the specific query {@link String} for foreign keys information
     *
     * @return specific foreign keys query String
     */
    String getForeignKeysQuery();
}
