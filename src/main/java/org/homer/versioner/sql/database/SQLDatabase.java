package org.homer.versioner.sql.database;

import org.homer.versioner.sql.entities.*;
import org.homer.versioner.sql.exception.ConnectionException;
import org.homer.versioner.sql.exception.DatabaseException;
import org.neo4j.graphdb.GraphDatabaseService;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static org.homer.versioner.sql.utils.Utils.newArrayList;

public abstract class SQLDatabase {

    private String hostName;
    private Long port;
    private String databaseName;

    private Connection connection;

    /**
     * It returns the specific database name
     *
     * @return specific database name
     */
    public abstract String getName();

    private Connection getConnection(String username, String password) {
       String connectionUrl = String.format(getConnectionUrl(), hostName, port, databaseName);
        try {
            return DriverManager.getConnection(connectionUrl, username, password);
        } catch (SQLException e) {
            throw new ConnectionException(e);
        }
    }

    protected abstract String getSchemaQuery();

    private String buildTablesQuery(String schema) {
        return String.format(getTablesQuery(), schema);
    }

    private String buildColumnsQuery(String schema, String table){
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

    public DatabaseNode getDatabase(GraphDatabaseService db) {
        //TODO will be called once in the init script on a persist logic unit
        return new DatabaseNode(db.createNode(), databaseName);
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

    public List<SchemaNode> getSchemas(GraphDatabaseService db) {

        try (ResultSet schemasRs = connection.createStatement().executeQuery(getSchemaQuery())) {
            List<SchemaNode> schemas = newArrayList();
            while(schemasRs.next()){
                schemas.add(new SchemaNode(db.createNode(), schemasRs));
            }
            return schemas;
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    public List<TableNode> getTables(SchemaNode schema) {

        try(ResultSet tablesRs = connection.createStatement().executeQuery(buildTablesQuery(schema.getName()))){
            List<TableNode> tables = newArrayList();
            while(tablesRs.next()){
                tables.add(new TableNode(tablesRs));
            }
            return tables;
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    public List<TableColumn> getColumns(SchemaNode schema, TableNode table) {

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

    //TODO remove database dependency
    public List<ForeignKey> getForeignKeys(DatabaseNode database) {
        try(ResultSet foreignKeysRs = connection.createStatement().executeQuery(getForeignKeysQuery())){
            List<ForeignKey> foreignKeys = newArrayList();
            while(foreignKeysRs.next()){
                foreignKeys.add(new ForeignKey(foreignKeysRs, database));
            }
            return foreignKeys;
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }
}
