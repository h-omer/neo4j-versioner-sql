package org.homer.versioner.sql.database;

public class Postgres implements SQLDatabase {

    private String brandName = "postgres";
    private String connectionURL = "jdbc:postgresql://%s:%s/%s";
    private String schemaQuery = "SELECT DISTINCT table_schema FROM information_schema.tables WHERE table_schema NOT IN ('pg_catalog', 'information_schema')";
    private String tablesQuery = "SELECT table_name FROM information_schema.tables WHERE table_schema = '%s'";
    private String columnsQuery = "SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_schema = '%s' AND table_name = '%s'";
    private String foreignKeysQuery = "SELECT DISTINCT tc.constraint_name, tc.table_name, tc.constraint_schema, kcu.column_name, ccu.table_name AS foreign_table_name, ccu.table_schema, ccu.column_name AS foreign_column_name FROM information_schema.table_constraints AS tc JOIN information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name JOIN information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name WHERE constraint_type = 'FOREIGN KEY'";

    @Override
    public String getDatabaseBrand() {
        return this.brandName;
    }

    @Override
    public String getConnectionURL(String hostName, Long port, String databaseName) {
        return String.format(this.connectionURL, hostName, port, databaseName);
    }

    @Override
    public String getSchemaQuery() {
        return this.schemaQuery;
    }

    @Override
    public String getTablesQuery(String schema) {
        return String.format(this.tablesQuery, schema);
    }

    @Override
    public String getColumnsQuery(String schema, String table) {
        return String.format(this.columnsQuery, schema, table);
    }

    @Override
    public String getForeignKeysQuery() {
        return this.foreignKeysQuery;
    }
}
