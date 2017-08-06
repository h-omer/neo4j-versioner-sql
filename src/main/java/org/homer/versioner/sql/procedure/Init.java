package org.homer.versioner.sql.procedure;

import org.homer.versioner.core.output.NodeOutput;
import org.homer.versioner.sql.importers.DatabaseImporter;
import org.homer.versioner.sql.importers.DatabaseImporterFactory;
import org.homer.versioner.sql.model.structure.*;
import org.homer.versioner.sql.persistence.Neo4jPersistence;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

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
            @Name("password") String password){

        DatabaseImporter databaseImporter = DatabaseImporterFactory.getSQLDatabase(dbName);
        databaseImporter.connect(hostname, port, databaseName, username, password);

        Database database = loadDatabaseFromDBMS(databaseImporter);

        databaseImporter.disconnect();

        Neo4jPersistence persistence = new Neo4jPersistence(db, log);
        Node databaseNode = persistence.persist(database);

        return Stream.of(new NodeOutput(databaseNode));
    }

    public static Database loadDatabaseFromDBMS(DatabaseImporter databaseImporter) {

        Database database = databaseImporter.getDatabase();

        List<Schema> schemas = databaseImporter.getSchemas();
        schemas.forEach(schema -> {

            database.addSchema(schema);
            List<Table> tables = databaseImporter.getTables(schema);
            tables.forEach(table -> {

                schema.addTable(table);

                List<TableColumn> columns = databaseImporter.getColumns(schema, table);
                columns.forEach(table::addColumn);
            });
        });

        List<ForeignKey> foreignKeys = databaseImporter.getForeignKeys();
        foreignKeys.forEach(foreignKey ->
                foreignKey.getSourceTable(database).ifPresent(sourceTable -> sourceTable.addForeignKey(foreignKey))
        );

        return database;
    }

}
