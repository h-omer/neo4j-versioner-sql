package org.homer.versioner.sql.importers;

import org.homer.versioner.sql.entities.*;
import org.homer.versioner.sql.exceptions.ConnectionException;
import org.homer.versioner.sql.exceptions.DatabaseException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static org.homer.versioner.sql.utils.Utils.newArrayList;

public abstract class DatabaseImporter {

    private String hostName;
    private Long port;
    private String databaseName;

    private Connection connection;

    private Connection getConnection(String username, String password) {
       String connectionUrl = String.format(getConnectionUrl(), hostName, port, databaseName);
        try {
            return DriverManager.getConnection(connectionUrl, username, password);
        } catch (SQLException e) {
            throw new ConnectionException(e);
        }
    }

    private String buildTablesQuery(String schema) {
        return String.format(getTablesQuery(), schema);
    }

    private String buildColumnsQuery(String schema, String table){
        return String.format(getColumnsQuery(), schema, table);
    }

    protected abstract String getConnectionUrl();

    protected abstract String getSchemaQuery();

    protected abstract String getTablesQuery();

    protected abstract String getColumnsQuery();

    protected abstract String getForeignKeysQuery();

    /**
     * It returns the specific database name
     *
     * @return specific database name
     */
    public abstract String getName();

    public Database getDatabase() {
        return new Database(databaseName, getName());
    }

    public void connect(String hostname, Long port, String databaseName, String username, String password) {
        this.hostName = hostname;
        this.port = port;
        this.databaseName = databaseName;
        this.connection = this.getConnection(username, password);
    }

    public void disconnect(){
        try {
            this.connection.close();
        } catch (SQLException e) {
            throw new ConnectionException(e);
        }
    }

    public List<Schema> getSchemas() {

        try (ResultSet schemasRs = connection.createStatement().executeQuery(getSchemaQuery())) {
            List<Schema> schemas = newArrayList();
            while(schemasRs.next()){
                schemas.add(new Schema(schemasRs));
            }
            return schemas;
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    public List<Table> getTables(Schema schema) {

        try(ResultSet tablesRs = connection.createStatement().executeQuery(buildTablesQuery(schema.getName()))){
            List<Table> tables = newArrayList();
            while(tablesRs.next()){
                tables.add(new Table(tablesRs));
            }
            return tables;
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    public List<TableColumn> getColumns(Schema schema, Table table) {

        try(ResultSet columnsRs = connection.createStatement().executeQuery(buildColumnsQuery(schema.getName(), table.getName()))){
            List<TableColumn> columns = newArrayList();
            while(columnsRs.next()){
                columns.add(new TableColumn(columnsRs));
            }
            return columns;
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    public List<ForeignKey> getForeignKeys() {

        try(ResultSet foreignKeysRs = connection.createStatement().executeQuery(getForeignKeysQuery())){
            List<ForeignKey> foreignKeys = newArrayList();
            while(foreignKeysRs.next()){
                foreignKeys.add(new ForeignKey(foreignKeysRs));
            }
            return foreignKeys;
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }
}
