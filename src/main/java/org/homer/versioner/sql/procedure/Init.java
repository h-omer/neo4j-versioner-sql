package org.homer.versioner.sql.procedure;

import org.homer.versioner.core.output.NodeOutput;
import org.homer.versioner.sql.database.SQLDatabase;
import org.homer.versioner.sql.database.SQLDatabaseFactory;
import org.homer.versioner.sql.entities.DatabaseNode;
import org.homer.versioner.sql.entities.ForeignKey;
import org.homer.versioner.sql.entities.SchemaNode;
import org.homer.versioner.sql.entities.TableNode;
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
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

        DatabaseNode database = new DatabaseNode(db.createNode(), databaseName);

        try (Connection con = sqlDatabase.getConnection(hostname, port, databaseName, username, password)) {
            try (ResultSet schemasRs = con.createStatement().executeQuery(sqlDatabase.getSchemaQuery())) {
                while (schemasRs.next()) {

                    SchemaNode schema = new SchemaNode(db.createNode(), schemasRs);
                    database.addSchema(schema);

                    try (ResultSet tablesRs = con.createStatement().executeQuery(sqlDatabase.buildTablesQuery(schema.getName()))) {
                        while (tablesRs.next()) {

                            TableNode table = new TableNode(tablesRs);

                            try (ResultSet columnRs = con.createStatement().executeQuery(sqlDatabase.buildColumnsQuery(schema.getName(), table.getName()))) {
                                while (columnRs.next()) {
                                    table.addColumn(columnRs);
                                }
                            }

                            new org.homer.versioner.core.builders.InitBuilder().withDb(db).withLog(log).build()
                                    .map(init -> init.init("Table", table.getAttributes(), table.getProperties(), "", 0L))
                                    .flatMap(Stream::findAny)
                                    .map(tblNode -> db.getNodeById(tblNode.node.getId()))
                                    .ifPresent(table::setNode);

                            schema.addTable(table);
                        }
                    }
                }
            }

            try (ResultSet foreignKeysRs = con.createStatement().executeQuery(sqlDatabase.getForeignKeysQuery())) {
                while (foreignKeysRs.next()) {

                    ForeignKey foreignKey = new ForeignKey(foreignKeysRs, database);
                    foreignKey.getSourceTable().ifPresent(sourceTable -> sourceTable.addForeignKey(foreignKey));
                }
            }
        }

        return Stream.of(new NodeOutput(database.getNode()));
    }

}
