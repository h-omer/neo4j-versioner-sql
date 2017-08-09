package org.homer.versioner.sql.persistence;

import lombok.AllArgsConstructor;
import org.homer.versioner.core.builders.GetBuilder;
import org.homer.versioner.sql.exceptions.DatabaseException;
import org.homer.versioner.sql.model.structure.*;
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.homer.versioner.sql.utils.Utils.newArrayList;

@AllArgsConstructor
public class Neo4jLoader {

	private GraphDatabaseService graphDb;
	private Neo4jVersionerCore neo4jVersionerCore;

	public Neo4jLoader(GraphDatabaseService graphDb, Log log) {
		this.graphDb = graphDb;
		neo4jVersionerCore = new Neo4jVersionerCore(graphDb, log);
	}

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

		Optional<Node> databaseNode = neo4jVersionerCore.findStateNode(database);
		if(databaseNode.isPresent()) {
			return StreamSupport.stream(databaseNode.get().getRelationships(Direction.OUTGOING, RelationshipType.withName("HAS_SCHEMA")).spliterator(), false)
					.map(Relationship::getEndNode)
					.filter(node -> node.hasLabel(Label.label("Schema")))
					.map(Schema::new)
					.collect(Collectors.toList());
		} else {
			return newArrayList();
		}
	}

	public List<Table> loadTables(Schema schema) {

		Optional<Node> schemaNode = neo4jVersionerCore.findStateNode(schema);
		if(schemaNode.isPresent()) {
			List<Table> tables = StreamSupport.stream(schemaNode.get().getRelationships(Direction.OUTGOING, RelationshipType.withName("HAS_TABLE")).spliterator(), false)
					.map(Relationship::getEndNode)
					.filter(node -> node.hasLabel(Label.label("Table")))
					.map(Table::new)
					.collect(Collectors.toList());

			tables.forEach(table -> {
				table.setColumns(getColumns(table));
				table.setForeignKeys(getForeignKeys(table));
			});

			return tables;
		} else {
			return newArrayList();
		}

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
