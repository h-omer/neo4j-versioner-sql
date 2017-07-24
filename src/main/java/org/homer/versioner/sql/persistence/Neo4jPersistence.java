package org.homer.versioner.sql.persistence;

import org.homer.versioner.sql.entities.*;
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;

import java.util.Optional;

public class Neo4jPersistence {

    private GraphDatabaseService graphDb;
    private Log log;
	private Neo4jVersionerCore neo4jVersionerCore;

	public Neo4jPersistence(GraphDatabaseService graphDb, Log log) {

		this.graphDb = graphDb;
		this.log = log;
		this.neo4jVersionerCore = new Neo4jVersionerCore(graphDb, log);
	}

    public Node persist(Database database) {

        Node databaseNode = persistDatabase(database);

		persistForeignKeys(database);

		return databaseNode;
    }

    private Node persistDatabase(Database database){

        Node databaseNode = neo4jVersionerCore.createVersionedNode(database);

		database.setNodeId(databaseNode.getId());
		neo4jVersionerCore.updateVersionedNode(database.getNodeId(), database); //FIXME remove when empty state on init

		database.getSchemas().forEach(schema ->
				neo4jVersionerCore.findStateNode(database).map(databaseStateNode ->
						databaseStateNode.createRelationshipTo(persistSchema(schema), RelationshipType.withName("HAS_SCHEMA")))
        );

		return databaseNode;
    }

    private Node persistSchema(Schema schema) {

        Node schemaNode = neo4jVersionerCore.createVersionedNode(schema);

		schema.setNodeId(schemaNode.getId());
		neo4jVersionerCore.updateVersionedNode(schema.getNodeId(), schema); //FIXME remove when empty state on init

        schema.getTables().forEach(table ->
				neo4jVersionerCore.findStateNode(schema).map(schemaStateNode ->
            			schemaStateNode.createRelationshipTo(persistTable(table), RelationshipType.withName("HAS_TABLE")))
        );

        return schemaNode;
    }

    private Node persistTable(Table table) {

        Node tableNode = neo4jVersionerCore.createVersionedNode(table);

        table.setNodeId(tableNode.getId());

        return tableNode;
    }

    private void persistForeignKeys(Database database) {

        database.getSchemas().forEach(schema ->
            schema.getTables().forEach(table -> {

                Optional<Node> sourceTableNode = neo4jVersionerCore.findStateNode(table);

                table.getForeignKeys().forEach(foreignKey ->
                        foreignKey.getDestinationTable(database).ifPresent(destinationTable -> {

                            Optional<Node> destinationTableNode = neo4jVersionerCore.findStateNode(destinationTable);

							if(sourceTableNode.isPresent() && destinationTableNode.isPresent()) {
								Relationship relationship = sourceTableNode.get().createRelationshipTo(destinationTableNode.get(), RelationshipType.withName("RELATION"));
								relationship.setProperty("constraint", foreignKey.getConstraintName());
								relationship.setProperty("source_column", foreignKey.getSourceColumnName());
								relationship.setProperty("destination_column", foreignKey.getDestinationColumnName());
							}
                        })
                );
            })
        );
    }
}
