package org.homer.versioner.sql.persistence;

import org.homer.versioner.sql.model.Action;
import org.homer.versioner.sql.model.ActionType;
import org.homer.versioner.sql.model.Versioned;
import org.homer.versioner.sql.model.structure.Database;
import org.homer.versioner.sql.model.structure.Schema;
import org.homer.versioner.sql.model.structure.Table;
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;

import java.util.Optional;

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

	//TODO process all the changes once at single persist() call
	public void persist(Action<? extends Versioned, ? extends Versioned> action) {

		if(action.is(ActionType.CREATE)) {

			Node newEntity = neo4jVersionerCore.createVersionedNode(action.getEntity());
			action.getParentEntity().ifPresent(parentEntity -> {

				Optional<Node> previousParentStateOpt = neo4jVersionerCore.findStateNode(parentEntity);
				neo4jVersionerCore.updateVersionedNode(parentEntity.getNodeId(), null);
				Optional<Node> newParentStateOpt = neo4jVersionerCore.findStateNode(parentEntity);
				//FIXME the relationships clone should not copy the versioner relationships
				if(previousParentStateOpt.isPresent() && newParentStateOpt.isPresent()){
					Iterable<Relationship> relationships = previousParentStateOpt.map(Node::getRelationships).orElse(newArrayList());
					relationships.forEach(relationship -> {
						Relationship newRelationship;
						if(relationship.getStartNodeId() == previousParentStateOpt.get().getId()){
							newRelationship = newParentStateOpt.get().createRelationshipTo(relationship.getEndNode(), relationship.getType());
						} else {
							newRelationship = relationship.getEndNode().createRelationshipTo(newParentStateOpt.get(), relationship.getType());
						}

						relationship.getAllProperties().forEach(newRelationship::setProperty);
					});
				}

				newParentStateOpt.ifPresent(newParentState -> newParentState.createRelationshipTo(newEntity, RelationshipType.withName(getRelationshipTypeName(action))));
			});
		} else if(action.is(ActionType.UPDATE)) {
			//TODO implement
		} else if(action.is(ActionType.DELETE)) {
			//TODO implement
		}
	}

	private String getRelationshipTypeName(Action<?, ?> action) {
		if(action.getParentEntity().isPresent() && action.getParentEntity().get() instanceof Database){
			return "HAS_SCHEMA";
		} else if(action.getParentEntity().isPresent() && action.getParentEntity().get() instanceof Schema) {
			return "HAS_TABLE";
		} else {
			return "";
		}
	}
}
