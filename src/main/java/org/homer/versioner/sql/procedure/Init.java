package org.homer.versioner.sql.procedure;

import org.homer.versioner.core.output.NodeOutput;
import org.homer.versioner.sql.database.SQLDatabase;
import org.homer.versioner.sql.database.SQLDatabaseFactory;
import org.homer.versioner.sql.entities.*;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Stream;

/**
 * Init class, it contains all the Procedures needed to initialize a Schema node
 */
public class Init {

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    @Procedure(value = "sql.versioner.init", mode = Mode.WRITE)
    @Description("sql.versioner.init(dbname, hostname, port, database, username, password) - Initialize a Database Version with its schemas, tables and relative columns.")
    public Stream<NodeOutput> init(
            @Name("dbName") String dbName,
            @Name("hostname") String hostname,
            @Name("port") Long port,
            @Name("database") String databaseName,
            @Name("username") String username,
            @Name("password") String password
    ) throws SQLException {

        SQLDatabase sqlDatabase = SQLDatabaseFactory.getSQLDatabase(dbName);
        sqlDatabase.connect(hostname, port, databaseName, username, password);

        DatabaseNode database = sqlDatabase.getDatabase(db);

        List<SchemaNode> schemas = sqlDatabase.getSchemas(db);
        schemas.forEach(schema -> {

            database.addSchema(schema);

            List<TableNode> tables = sqlDatabase.getTables(schema);
            tables.forEach(table -> {

                List<TableColumn> columns = sqlDatabase.getColumns(schema, table);
                columns.forEach(table::addColumn);

                //TODO refactor
                new org.homer.versioner.core.builders.InitBuilder().withDb(db).withLog(log).build()
                        .map(init -> init.init("Table", table.getAttributes(), table.getProperties(), "", 0L))
                        .flatMap(Stream::findAny)
                        .map(tblNode -> db.getNodeById(tblNode.node.getId()))
                        .ifPresent(table::setNode);

                schema.addTable(table);
            });
        });

        List<ForeignKey> foreignKeys = sqlDatabase.getForeignKeys(database);
        foreignKeys.forEach(foreignKey ->
                foreignKey.getSourceTable().ifPresent(sourceTable -> sourceTable.addForeignKey(foreignKey))
        );

        sqlDatabase.disconnect();

        //TODO call persistence unit to persist the whole database

        return Stream.of(new NodeOutput(database.getNode()));
    }

}
