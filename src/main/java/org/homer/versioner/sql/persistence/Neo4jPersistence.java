package org.homer.versioner.sql.persistence;

import org.homer.versioner.sql.model.action.ActionType;
import org.homer.versioner.sql.model.action.SchemaAction;
import org.homer.versioner.sql.model.action.TableAction;
import org.homer.versioner.sql.model.structure.Database;
import org.homer.versioner.sql.model.structure.Schema;
import org.homer.versioner.sql.model.structure.Table;
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;

import java.util.List;
import java.util.Optional;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toList;
import static org.homer.versioner.sql.utils.Utils.newArrayList;

public class Neo4jPersistence {

	private GraphDatabaseService graphDb;
	private Log log;
	private Neo4jVersionerCore neo4jVersionerCore;

	public Neo4jPersistence(GraphDatabaseService graphDb, Log log) {

		this.log = log;
		this.graphDb = graphDb;
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

		database.getSchemas().forEach(schema ->
				neo4jVersionerCore.findStateNode(database).map(databaseStateNode ->
						databaseStateNode.createRelationshipTo(persistSchema(schema), RelationshipType.withName("HAS_SCHEMA")))
        );

		return databaseNode;
    }

    private Node persistSchema(Schema schema) {

        Node schemaNode = neo4jVersionerCore.createVersionedNode(schema);

		schema.setNodeId(schemaNode.getId());

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

	public void persist(TableAction action) {

		if(action.is(ActionType.CREATE)){
			Node newEntity = neo4jVersionerCore.createVersionedNode(action.getEntity());
			Optional<Node> parentEntityStatusOpt = newStatusCloningCurrentOne(action.getParentEntity().map(a -> graphDb.getNodeById(a.getNodeId())), "HAS_TABLE");

			parentEntityStatusOpt.ifPresent(parentEntityStatus ->
					parentEntityStatus.createRelationshipTo(newEntity, RelationshipType.withName("HAS_TABLE")));

			neo4jVersionerCore.findStateNode(newEntity.getId()).ifPresent(state -> {
				action.getEntity().getForeignKeys().forEach(foreignKey -> {
					Optional<Node> newDestinationTableStatusOpt = newStatusCloningCurrentOne(Optional.of(graphDb.getNodeById(foreignKey.getDestinationTableId())), "RELATION");
					newDestinationTableStatusOpt.ifPresent(newDestinationTableStatus -> {
						neo4jVersionerCore.findStateNode(newEntity.getId()).ifPresent(newEntityStatus -> {
							Relationship relation = newEntityStatus.createRelationshipTo(newDestinationTableStatus, RelationshipType.withName("RELATION"));
							relation.setProperty("constraint", foreignKey.getConstraintName());
							relation.setProperty("source_column", foreignKey.getSourceColumnName());
							relation.setProperty("destination_column", foreignKey.getDestinationColumnName());
						});
					});
				});
			});
		}
		//TODO implement UPDATE
		//TODO implement DELETE
	}

	private Optional<Node> newStatusCloningCurrentOne(Optional<Node> entityOpt, String relationshipName) {

		//TODO remove relations to nodes not CURRENT
		Optional<Node> currentState = entityOpt.flatMap(entity -> neo4jVersionerCore.findStateNode(entity.getId()));
		Optional<Node> newState = entityOpt.flatMap(entity -> {
			neo4jVersionerCore.updateVersionedNode(entity.getId(), null);
			return neo4jVersionerCore.findStateNode(entity.getId());
		});

		if (currentState.isPresent() && newState.isPresent()) {
			List<Relationship> relationships = currentState.map(n ->
					StreamSupport.stream(n.getRelationships(RelationshipType.withName(relationshipName)).spliterator(), false).collect(toList()))
					.orElse(newArrayList());
			relationships.forEach(relationship -> {
				Relationship newRelationship;
				if (relationship.getStartNodeId() == currentState.get().getId()) {
					newRelationship = newState.get().createRelationshipTo(relationship.getEndNode(), relationship.getType());
				} else {
					newRelationship = relationship.getStartNode().createRelationshipTo(newState.get(), relationship.getType());
				}

				relationship.getAllProperties().forEach(newRelationship::setProperty);
			});

			currentState.get().getAllProperties().forEach((key, value) -> newState.get().setProperty(key, value));
		}


		return newState;
	}

	public void persist(SchemaAction action) {
		//TODO implement
	}
}
