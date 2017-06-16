package org.homer.graph.sql.versioner;

import org.homer.versioner.core.output.NodeOutput;
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
    @Description("sql.versioner.init(schema) - Create a Database node by importing from the database.")
    public Stream<NodeOutput> init(
            @Name("hostname") String hostname,
            @Name("port") Long port,
            @Name("database") String databaseName,
            @Name("username") String username,
            @Name("password") String password
    ) throws SQLException {
        Node database = db.createNode();
        database.addLabel(Label.label("Database"));
        database.setProperty("name", databaseName);


        try (Connection con = DriverManager.getConnection(String.format("jdbc:postgresql://%s:%s/%s", hostname, port, databaseName), username, password)) {

            try (ResultSet schemasRs = con.createStatement().executeQuery("SELECT DISTINCT table_schema FROM information_schema.tables WHERE table_schema NOT IN ('pg_catalog', 'information_schema')")) {
                while (schemasRs.next()) {
                    Node schema = db.createNode();
                    schema.addLabel(Label.label("Schema"));
                    schema.setProperty("name", schemasRs.getString(1));

                    database.createRelationshipTo(schema, RelationshipType.withName("HAS_SCHEMA"));

                    try (ResultSet tablesRs = con.createStatement().executeQuery(String.format("SELECT table_name FROM information_schema.tables WHERE table_schema = '%s'", schema.getProperty("name")))) {
                        while (tablesRs.next()) {
                            Map<String, Object> tableAttributes = new HashMap<>();
                            tableAttributes.put("name", tablesRs.getString(1));

                            Map<String, Object> tableColumns = new HashMap<>();

                            try (ResultSet columnRs = con.createStatement().executeQuery(String.format("SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_schema = '%s' AND table_name = '%s'", schema.getProperty("name"), tableAttributes.get("name")))) {
                                while (columnRs.next()) {
                                    List<String> attributes = new LinkedList<>();
                                    attributes.add(columnRs.getString(2));
                                    if (columnRs.getBoolean(3)) {
                                        attributes.add("NOT NULL");
                                    }
                                    tableColumns.put(columnRs.getString(1), attributes.toArray(new String[attributes.size()]));
                                }
                            }

                            new org.homer.versioner.core.builders.InitBuilder().withDb(db).withLog(log).build()
                                    .map(init -> init.init("Table", tableAttributes, tableColumns, "", 0L))
                                    .flatMap(Stream::findAny)
                                    .ifPresent(tableNode -> schema.createRelationshipTo(db.getNodeById(tableNode.node.getId()), RelationshipType.withName("HAS_TABLE")));
                        }
                    }
                }
            }

            try (ResultSet foreignKeysRs = con.createStatement().executeQuery("SELECT DISTINCT tc.constraint_name, tc.table_name, tc.constraint_schema, kcu.column_name, ccu.table_name AS foreign_table_name, ccu.table_schema, ccu.column_name AS foreign_column_name FROM information_schema.table_constraints AS tc JOIN information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name JOIN information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name WHERE constraint_type = 'FOREIGN KEY'")) {
                while (foreignKeysRs.next()) {
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
            }
        }

        return Stream.of(new NodeOutput(database));
    }

    private Node getChildTableWithName(Node node, String tableName) {
        return StreamSupport.stream(node.getRelationships(RelationshipType.withName("HAS_TABLE")).spliterator(), false)
                .filter(r -> tableName.equals(r.getEndNode().getProperty("name"))).map(Relationship::getEndNode)
                .flatMap(tableEntity -> new org.homer.versioner.core.builders.GetBuilder().build().get().getCurrentState(tableEntity))
                .map(tableCurrent -> db.getNodeById(tableCurrent.node.getId()))
                .findAny().orElse(null);
    }
}
