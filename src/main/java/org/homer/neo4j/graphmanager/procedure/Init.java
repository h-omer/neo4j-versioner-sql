package org.homer.neo4j.graphmanager.procedure;

import org.homer.neo4j.graphmanager.output.NodeOutput;
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.sql.*;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
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

    @Procedure(value = "sql.manager.init", mode = Mode.WRITE)
    @Description("sql.manager.init(schema) - Create a Database node by importing from the database.")
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

			try(ResultSet schemasRs = con.createStatement().executeQuery("SELECT DISTINCT table_schema FROM information_schema.tables WHERE table_schema NOT IN ('pg_catalog', 'information_schema')")){
				while(schemasRs.next()){
					Node schema = db.createNode();
					schema.addLabel(Label.label("Schema"));
					schema.setProperty("name", schemasRs.getString(1));

					database.createRelationshipTo(schema, RelationshipType.withName("HAS_SCHEMA"));

					try(ResultSet tablesRs = con.createStatement().executeQuery(String.format("SELECT table_name FROM information_schema.tables WHERE table_schema = '%s'", schema.getProperty("name")))) {
						while (tablesRs.next()) {
							Node table = db.createNode();
							table.addLabel(Label.label("Table"));
							table.setProperty("_name", tablesRs.getString(1));

							schema.createRelationshipTo(table, RelationshipType.withName("HAS_TABLE"));

							try(ResultSet columnRs = con.createStatement().executeQuery(String.format("SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_schema = '%s' AND table_name = '%s'", schema.getProperty("name"), table.getProperty("_name")))){
								while(columnRs.next()){
									List<String> attributes = new LinkedList<>();
									attributes.add(columnRs.getString(2));
									if(columnRs.getBoolean(3)){
										attributes.add("NOT NULL");
									}
									table.setProperty(columnRs.getString(1), attributes.toArray(new String[attributes.size()]));
								}
							}

							try(ResultSet foreignKeysRs = con.createStatement().executeQuery(String.format("SELECT tc.constraint_name, tc.table_name, kcu.column_name, ccu.table_name AS foreign_table_name, ccu.column_name AS foreign_column_name FROM information_schema.table_constraints AS tc JOIN information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name JOIN information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name WHERE constraint_type = 'FOREIGN KEY' AND tc.table_name='%s' AND tc.table_schema='%s'", table.getProperty("_name"), schema.getProperty("name")))){
								while(foreignKeysRs.next()){
									Optional<Node> foreignTable = Optional.ofNullable(db.findNode(Label.label("Table"), "_name", foreignKeysRs.getString(4)));
									if(foreignTable.isPresent()) {
										Relationship relationship = table.createRelationshipTo(foreignTable.get(), RelationshipType.withName("RELATION"));
										relationship.setProperty("constraint", foreignKeysRs.getString(1));
										relationship.setProperty("source_column", foreignKeysRs.getString(3));
										relationship.setProperty("destination_column", foreignKeysRs.getString(5));
									}
								}
							}
						}
					}
				}
			}
        }

        return Stream.of(new NodeOutput(database));
    }
}
