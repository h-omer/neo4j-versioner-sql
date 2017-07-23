package org.homer.versioner.sql.persistence;

import lombok.AllArgsConstructor;
import org.homer.versioner.core.builders.GetBuilder;
import org.homer.versioner.core.output.NodeOutput;
import org.homer.versioner.sql.entities.*;
import org.homer.versioner.sql.exceptions.DatabaseException;
import org.neo4j.graphdb.*;

import javax.management.relation.Relation;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@AllArgsConstructor
public class Neo4jLoader {

	private GraphDatabaseService graphDb;

	public Database loadDatabase() {
		return new Database(loadDatabaseNode());
	}

	private Node loadDatabaseNode() {
		return graphDb
				.findNodes(Label.label("Database")).stream()
				.findFirst()
				.orElseThrow(() -> new DatabaseException("Database node not found"));
	}

	public List<Schema> loadSchemas(Database database) {

		Node databaseNode = graphDb.getNodeById(database.getNodeId());
		return StreamSupport.stream(databaseNode.getRelationships(Direction.OUTGOING, RelationshipType.withName("HAS_SCHEMA")).spliterator(), false)
				.map(Relationship::getEndNode)
				.filter(node -> node.hasLabel(Label.label("Schema")))
				.map(Schema::new)
				.collect(Collectors.toList());
	}

	public List<Table> loadTables(Schema schema) {

		Node schemaNode = graphDb.getNodeById(schema.getNodeId());
		List<Table> tables = StreamSupport.stream(schemaNode.getRelationships(Direction.OUTGOING, RelationshipType.withName("HAS_TABLE")).spliterator(), false)
				.map(Relationship::getEndNode)
				.filter(node -> node.hasLabel(Label.label("Schema")))
				.map(Table::new)
				.collect(Collectors.toList());

		tables.forEach(table -> {
			table.setColumns(getColumns(table));
			table.setForeignKeys(getForeignKeys(table));
		});

		return tables;
	}

	private List<TableColumn> getColumns(Table table) {

		return new GetBuilder().build()
				.flatMap(get -> get.getCurrentState(graphDb.getNodeById(table.getNodeId())).findFirst())
				.map(nodeOutput -> nodeOutput.node)
				.map(TableColumn::build)
				.orElseThrow(() -> new DatabaseException("State node not found"));
	}

	private List<ForeignKey> getForeignKeys(Table table) {

		Node tableNode = graphDb.getNodeById(table.getNodeId());
		return StreamSupport.stream(tableNode.getRelationships(Direction.OUTGOING, RelationshipType.withName("RELATION")).spliterator(), false)
				.map(ForeignKey::new)
				.collect(Collectors.toList());
	}
}
