package org.homer.versioner.sql.integration;

import org.homer.versioner.sql.importers.DatabaseImporter;
import org.homer.versioner.sql.model.structure.*;
import org.homer.versioner.sql.persistence.Neo4jVersionerCore;
import org.neo4j.graphdb.*;
import org.parboiled.common.Tuple2;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.homer.versioner.sql.utils.Utils.newArrayList;
import static org.homer.versioner.sql.utils.Utils.newHashMap;
import static org.mockito.Mockito.when;

public class TestData {

	public static final String MOCKED_DATABASE_NAME = "TEST_DATABASE";

	public static class FULLDATABASE {
		public static List<ForeignKey> FOREIGN_KEYS;
		public static Map<String, List<TableColumn>> TABLES_COLUMNS;
		public static List<Table> TABLES;
		public static List<Schema> SCHEMAS;
		public static Database DATABASE;

		static {
			try {
				FOREIGN_KEYS = newArrayList(
						ForeignKey.builder().sourceTableName("testTable2").destinationTableName("testTable1").sourceColumnName("testTable2Column2").destinationColumnName("testTable1Column1").sourceSchemaName("testSchema").destinationSchemaName("testSchema")
								.constraintName("testConstraintName").build());

				TABLES_COLUMNS = newHashMap("testTable1", newArrayList(TableColumn.builder()
						.name("testTable1Column1")
						.attributes(newArrayList("PRIMARY KEY", "NOT NULL"))
						.build()),
						"testTable2", newArrayList(
								TableColumn.builder()
										.name("testTable2Column1")
										.attributes(newArrayList("PRIMARY KEY", "NOT NULL"))
										.build(),
								TableColumn.builder()
										.name("testTable2Column2")
										.attributes(newArrayList("NOT NULL"))
										.build()
						));

				TABLES = newArrayList(
						Table.builder()
								.name("testTable1")
								.foreignKeys(newArrayList())
								.columns(newArrayList())
								.build(),
						Table.builder()
								.name("testTable2")
								.foreignKeys(newArrayList())
								.columns(newArrayList())
								.build()
				);

				SCHEMAS = newArrayList(Schema.builder()
						.name("testSchema")
						.tables(newArrayList())
						.build()
				);

				DATABASE = Database.builder()
						.databaseType(MOCKED_DATABASE_NAME)
						.name("testDatabase")
						.schemas(newArrayList())
						.build();

			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static void initMocks(DatabaseImporter mockedDatabaseImporter, Database database, List<Schema> schemas, List<Table> tables,
			Map<String, List<TableColumn>> tableColumns, List<ForeignKey> foreignKeys) throws SQLException {

		when(mockedDatabaseImporter.getDatabase()).thenReturn(database);
		when(mockedDatabaseImporter.getSchemas()).thenReturn(schemas);
		schemas.forEach(schema -> {
			when(mockedDatabaseImporter.getTables(schema)).thenReturn(tables);
			tables.forEach(table ->
					when(mockedDatabaseImporter.getColumns(schema, table)).thenReturn(tableColumns.get(table.getName())));
		});

		when(mockedDatabaseImporter.getForeignKeys()).thenReturn(foreignKeys);

	}

	public static void assertDatabaseNode(GraphDatabaseService neo4j, Neo4jVersionerCore neo4jVersionerCore, Map<String, Object> properties, Integer schemasNumber) {

		//Assert structure
		List<Node> databaseList = neo4j.findNodes(Label.label("Database")).stream().collect(toList());
		assertThat(databaseList).hasSize(1);

		Node databaseNode = databaseList.stream().findFirst().orElse(null);
		assertThat(databaseNode.getAllProperties())
				.containsAllEntriesOf(properties);

		//Assert relationships
		Optional<Node> databaseState = neo4jVersionerCore.findStateNode(databaseNode.getId());

		assertThat(databaseState).isPresent();
		assertThat(databaseState.get().getRelationships(RelationshipType.withName("HAS_SCHEMA")))
				.hasSize(schemasNumber)
				.anySatisfy(relationship -> assertThat(relationship.getStartNode()).isEqualTo(databaseState.get()));
	}

	public static void assertSchemaNode(GraphDatabaseService neo4j, Neo4jVersionerCore neo4jVersionerCore, Map<String, Object> properties, Integer tablesNumber) {

		//Assert structure
		List<Node> schemaList = neo4j.findNodes(Label.label("Schema")).stream().collect(toList());
		assertThat(schemaList)
				.hasSize(1)
				.allSatisfy(schema -> assertThat(schema.getAllProperties())
						.containsAllEntriesOf(properties));

		//Test schema relationships
		Optional<Node> schemaState = neo4jVersionerCore.findStateNode(schemaList.get(0).getId());

		assertThat(schemaState)
				.isPresent()
				.satisfies(schemaNode -> assertThat(schemaNode.get().getRelationships(RelationshipType.withName("HAS_TABLE")))
						.hasSize(tablesNumber)
						.allSatisfy(relationship -> assertThat(relationship.getStartNode()).isEqualTo(schemaNode.get())));
	}

	public static void assertTables(GraphDatabaseService neo4j, Neo4jVersionerCore neo4jVersionerCore, Map<String, Map<String, String[]>> tablesProperties) {
		//Test tables structure
		List<Node> tableList = neo4j.findNodes(Label.label("Table")).stream().collect(toList());
		assertThat(tableList)
				.hasSize(tablesProperties.size());

		tablesProperties.forEach((tableName, properties) ->
				assertThat(tableList).anySatisfy(table ->
					assertThat(table.getAllProperties()).containsAllEntriesOf(newHashMap("name", tableName))));

		//Test tables relationships
		List<Optional<Node>> tableStateList = tableList.stream().map(table -> neo4jVersionerCore.findStateNode(table.getId())).collect(toList());

		tablesProperties.forEach((tableName, properties) -> {
			assertThat(tableStateList)
					.anySatisfy(tableStateNodeOpt ->
							assertThat(tableStateNodeOpt)
									.isPresent()
									.satisfies(tableStateNode ->
											assertThat(tableStateNode.get().getAllProperties()).hasSize(properties.size()).containsAllEntriesOf(
													properties)));
		});
	}

	public static void assertForeignKeys(GraphDatabaseService neo4j, Neo4jVersionerCore neo4jVersionerCore, Map<Tuple2<String, String>, Map<String, String>> relationships) {

		ResourceIterator<Optional<Node>> tablesStateList = neo4j.findNodes(Label.label("Table")).map(table -> neo4jVersionerCore.findStateNode(table.getId()));

		List<Relationship> foreignKeysList = tablesStateList.stream()
				.map(tableStateOpt -> tableStateOpt.get().getRelationships(Direction.OUTGOING, RelationshipType.withName("RELATION")))
				.flatMap(list -> StreamSupport.stream(list.spliterator(), false))
				.collect(toList());

		assertThat(foreignKeysList)
				.hasSize(relationships.size());

		relationships.forEach((keys, description) -> assertThat(foreignKeysList).anySatisfy(foreignKey -> {
			assertThat(foreignKey.getStartNode().hasProperty(keys.a)).isTrue();
			assertThat(foreignKey.getEndNode().hasProperty(keys.b)).isTrue();
			assertThat(foreignKey.getAllProperties()).containsAllEntriesOf(description);
		}));
	}

}
