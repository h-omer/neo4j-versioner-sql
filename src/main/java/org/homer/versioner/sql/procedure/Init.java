package org.homer.versioner.sql.procedure;

import org.homer.versioner.core.output.NodeOutput;
import org.homer.versioner.sql.database.DatabaseNotFoundException;
import org.homer.versioner.sql.database.SQLDatabase;
import org.homer.versioner.sql.database.SQLDatabaseFactory;
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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
        // Initializing SQL Database specific generator
        SQLDatabase sqlDatabase = SQLDatabaseFactory.getSQLDatabase(dbName);
        if (sqlDatabase == null) {
            throw new DatabaseNotFoundException();
        }
        sqlDatabase.getSchemaQuery();
        // Creating SQLDatabase Node
        Node database = db.createNode();
        database.addLabel(Label.label("SQLDatabase"));
        database.setProperty("name", databaseName);

        try (Connection con = DriverManager.getConnection(String.format(sqlDatabase.getConnectionURL(hostname, port, databaseName), username, password))) {
            try (ResultSet schemasRs = con.createStatement().executeQuery(sqlDatabase.getSchemaQuery())) {
                while (schemasRs.next()) {
                    // Creating Schema node related to database
                    Node schema = processSchema(database, schemasRs);

                    try (ResultSet tablesRs = con.createStatement().executeQuery(sqlDatabase.getTablesQuery(schema.getProperty("name").toString()))) {
                        // Preparing for Init procedure
                        while (tablesRs.next()) {
                            // Initializing Entity (Table) node properties
                            Map<String, Object> tableAttributes = new HashMap<>();
                            tableAttributes.put("name", tablesRs.getString(1));

                            // Initializing State node properties
                            Map<String, Object> tableColumns = new HashMap<>();

                            try (ResultSet columnRs = con.createStatement().executeQuery(sqlDatabase.getColumnsQuery(schema.getProperty("name").toString(), tableAttributes.get("name").toString()))) {
                                while (columnRs.next()) {
                                    processColumns(tableColumns, columnRs);
                                }
                            }

                            // Creating Entity node (Table)
                            new org.homer.versioner.core.builders.InitBuilder().withDb(db).withLog(log).build()
                                    .map(init -> init.init("Table", tableAttributes, tableColumns, "", 0L))
                                    .flatMap(Stream::findAny)
                                    .ifPresent(tableNode -> schema.createRelationshipTo(db.getNodeById(tableNode.node.getId()), RelationshipType.withName("HAS_TABLE")));
                        }
                    }
                }
            }

            try (ResultSet foreignKeysRs = con.createStatement().executeQuery(sqlDatabase.getForeignKeysQuery())) {
                while (foreignKeysRs.next()) {
                    processForeignKeys(foreignKeysRs);
                }
            }
        }

        return Stream.of(new NodeOutput(database));
    }

    /**
     * It process a schema result set, updating the graph
     *
     * @param database
     * @param schemasRs
     * @return schema
     * @throws SQLException
     */
    private Node processSchema(Node database, ResultSet schemasRs) throws SQLException {
        Node schema = db.createNode();
        schema.addLabel(Label.label("Schema"));
        schema.setProperty("name", schemasRs.getString(1));

        database.createRelationshipTo(schema, RelationshipType.withName("HAS_SCHEMA"));
        return schema;
    }

    /**
     * It process a columns result set
     *
     * @param tableColumns
     * @param columnRs
     * @throws SQLException
     */
    private void processColumns(Map<String, Object> tableColumns, ResultSet columnRs) throws SQLException {
        List<String> attributes = new LinkedList<>();
        attributes.add(columnRs.getString(2));
        if (columnRs.getBoolean(3)) {
            attributes.add("NOT NULL");
        }
        tableColumns.put(columnRs.getString(1), attributes.toArray(new String[attributes.size()]));
    }

    /**
     * It process a foreign keys result set, updating the graph
     *
     * @param foreignKeysRs
     * @throws SQLException
     */
    private void processForeignKeys(ResultSet foreignKeysRs) throws SQLException {
        String constraintName = foreignKeysRs.getString(1);
        String sourceTableName = foreignKeysRs.getString(2);
        String sourceSchemaName = foreignKeysRs.getString(3);
        String sourceColumnName = foreignKeysRs.getString(4);
        String destinationTableName = foreignKeysRs.getString(5);
        String destinationSchemaName = foreignKeysRs.getString(6);
        String destinationColumnName = foreignKeysRs.getString(7);

        Optional<Node> sourceTable = db.findNodes(Label.label("Schema"), "name", sourceSchemaName).stream()
                .map(n -> getChildTableWithName(n, sourceTableName)).findFirst();

        Optional<Node> destinationTable = db.findNodes(Label.label("Schema"), "name", destinationSchemaName).stream()
                .map(n -> getChildTableWithName(n, destinationTableName)).findFirst();

        if (sourceTable.isPresent() && destinationTable.isPresent()) {
            Relationship relationship = sourceTable.get().createRelationshipTo(destinationTable.get(), RelationshipType.withName("RELATION"));
            relationship.setProperty("constraint", constraintName);
            relationship.setProperty("source_column", sourceColumnName);
            relationship.setProperty("destination_column", destinationColumnName);
        }
    }

    /**
     * By a given node and table name, it returns a table with its name
     *
     * @param node
     * @param tableName
     * @return node
     */
    private Node getChildTableWithName(Node node, String tableName) {
        return StreamSupport.stream(node.getRelationships(RelationshipType.withName("HAS_TABLE")).spliterator(), false)
                .filter(r -> tableName.equals(r.getEndNode().getProperty("name"))).map(Relationship::getEndNode)
                .flatMap(tableEntity -> new org.homer.versioner.core.builders.GetBuilder().build().get().getCurrentState(tableEntity))
                .map(tableCurrent -> db.getNodeById(tableCurrent.node.getId()))
                .findAny().orElse(null);
    }
}
