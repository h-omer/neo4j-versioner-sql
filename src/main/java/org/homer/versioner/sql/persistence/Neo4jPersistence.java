package org.homer.versioner.sql.persistence;

import lombok.AllArgsConstructor;
import org.homer.versioner.sql.entities.Database;
import org.homer.versioner.sql.entities.Schema;
import org.homer.versioner.sql.entities.Table;
import org.homer.versioner.sql.exceptions.DatabasePersistenceException;
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;

import java.util.stream.Stream;

@AllArgsConstructor
public class Neo4jPersistence {

    private GraphDatabaseService graphDb;
    private Log log;

    public Node persist(Database database) {

        Node databaseNode = graphDb.createNode();

        databaseNode.addLabel(Label.label("Database"));
		databaseNode.setProperty("name", database.getName());

		database.getSchemas().forEach(schema ->
                databaseNode.createRelationshipTo(persistSchema(schema), RelationshipType.withName("HAS_SCHEMA"))
        );

		database.setNodeId(databaseNode.getId());

		persistForeignKeys(database);

		return databaseNode;
    }

    private Node persistSchema(Schema schema) {
        Node schemaNode = graphDb.createNode();

        schemaNode.addLabel(Label.label("Schema"));
		schemaNode.setProperty("name", schema.getName());

		schema.setNodeId(schemaNode.getId());

        schema.getTables().forEach(table ->
            schemaNode.createRelationshipTo(persistTable(table), RelationshipType.withName("HAS_TABLE"))
        );

        return schemaNode;
    }

    private Node persistTable(Table table) {

        Node tableNode = new org.homer.versioner.core.builders.InitBuilder().withDb(graphDb).withLog(log).build()
                .map(init -> init.init("Table", table.getAttributes(), table.getProperties(), "", 0L))
                .flatMap(Stream::findFirst)
                .map(node -> node.node)
                .orElseThrow(() -> new DatabasePersistenceException("Cannot persist table " + table));

        table.setNodeId(tableNode.getId());

        return tableNode;
    }

    private void persistForeignKeys(Database database) {

        database.getSchemas().forEach(schema ->
            schema.getTables().forEach(table -> {

                Node sourceTableNode = graphDb.getNodeById(table.getNodeId());

                table.getForeignKeys().forEach(foreignKey ->
                        foreignKey.getDestinationTable(database).ifPresent(destinationTable -> {

                            Node destinationTableNode = graphDb.getNodeById(destinationTable.getNodeId());

                            Relationship relationship = sourceTableNode.createRelationshipTo(destinationTableNode, RelationshipType.withName("RELATION"));
                            relationship.setProperty("constraint", foreignKey.getConstraintName());
		                    relationship.setProperty("source_column", foreignKey.getSourceColumnName());
		                    relationship.setProperty("destination_column", foreignKey.getDestinationColumnName());
                        })
                );
            })
        );
    }
}
