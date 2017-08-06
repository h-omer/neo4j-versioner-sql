package org.homer.versioner.sql.integration;

import org.assertj.core.api.Condition;
import org.homer.versioner.sql.importers.DatabaseImporter;
import org.homer.versioner.sql.importers.DatabaseImporterFactory;
import org.homer.versioner.sql.importers.PostgresDatabaseImporter;
import org.homer.versioner.sql.model.structure.*;
import org.homer.versioner.sql.persistence.Neo4jVersionerCore;
import org.homer.versioner.sql.procedure.Init;
import org.junit.*;
import org.neo4j.graphdb.*;
import org.neo4j.harness.junit.Neo4jRule;
import org.neo4j.logging.BufferingLog;

import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.homer.versioner.sql.utils.Utils.newArrayList;
import static org.homer.versioner.sql.utils.Utils.newHashMap;
import static org.mockito.Mockito.*;

public class InitIntegrationTest {

	@Rule
	public Neo4jRule neo4j = new Neo4jRule().withProcedure(Init.class);

	private Neo4jVersionerCore neo4jVersionerCore;

	private static List<DatabaseImporter> databaseList = newArrayList();
	private static final String MOCKED_DATABASE_NAME = "TEST_DATABASE";

	private DatabaseImporter mockedDatabaseImporter;

	@BeforeClass
	public static void setUpClass() throws NoSuchFieldException, IllegalAccessException {

		Field registeredDatabases = DatabaseImporterFactory.class.getDeclaredField("REGISTERED_DATABASES");
		registeredDatabases.setAccessible(true);

		databaseList.addAll((List<DatabaseImporter>) registeredDatabases.get(null));

		registeredDatabases.set(null, databaseList);

		registeredDatabases.setAccessible(false);
	}

	@Before
	public void setUp() throws NoSuchFieldException, IllegalAccessException {

		neo4jVersionerCore = new Neo4jVersionerCore(neo4j.getGraphDatabaseService(), new BufferingLog());

		mockedDatabaseImporter = mock(PostgresDatabaseImporter.class);
		when(mockedDatabaseImporter.getName()).thenReturn(MOCKED_DATABASE_NAME);
		databaseList.add(mockedDatabaseImporter);
	}

	@After
	public void tearDown() {
		databaseList.removeIf(databaseImporter -> databaseImporter.getName().equals(MOCKED_DATABASE_NAME));
	}

	@Test
	public void shouldInjectCorrectlyTheMockIntoFactory() {

		assertThat(DatabaseImporterFactory.getSQLDatabase(MOCKED_DATABASE_NAME)).isEqualTo(mockedDatabaseImporter);
	}

	@Test
	public void shouldInitCorrectlyTheGraph() throws SQLException {

		List<ForeignKey> foreignKeys = newArrayList(ForeignKey.builder()
				.sourceTableName("testTable2")
				.destinationTableName("testTable1")
				.sourceColumnName("testTable2Column2")
				.destinationColumnName("testTable1Column1")
				.sourceSchemaName("testSchema")
				.destinationSchemaName("testSchema")
				.constraintName("testConstraintName")
				.build()
		);

		List<TableColumn> columns2 = newArrayList(
				TableColumn.builder()
						.name("testTable2Column1")
						.attributes(newArrayList("PRIMARY KEY", "NOT NULL"))
						.build(),
				TableColumn.builder()
						.name("testTable2Column2")
						.attributes(newArrayList("NOT NULL"))
						.build()
		);

		List<TableColumn> columns1 = newArrayList(TableColumn.builder()
				.name("testTable1Column1")
				.attributes(newArrayList("PRIMARY KEY", "NOT NULL"))
				.build()
		);

		List<Table> tables = newArrayList(
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

		List<Schema> schemas = newArrayList(Schema.builder()
				.name("testSchema")
				.tables(newArrayList())
				.build()
		);

		Database database = Database.builder()
				.databaseType(MOCKED_DATABASE_NAME)
				.name("testDatabase")
				.schemas(newArrayList())
				.build();

		when(mockedDatabaseImporter.getDatabase()).thenReturn(database);
		when(mockedDatabaseImporter.getSchemas()).thenReturn(schemas);
		when(mockedDatabaseImporter.getTables(schemas.get(0))).thenReturn(tables);
		when(mockedDatabaseImporter.getColumns(schemas.get(0), tables.get(0))).thenReturn(columns1);
		when(mockedDatabaseImporter.getColumns(schemas.get(0), tables.get(1))).thenReturn(columns2);
		when(mockedDatabaseImporter.getForeignKeys()).thenReturn(foreignKeys);


		neo4j.getGraphDatabaseService().execute("CALL sql.versioner.init('" + MOCKED_DATABASE_NAME + "', '', 0, '', '', '')");


		//Test database structure
		List<Node> databaseList = neo4j.getGraphDatabaseService().findNodes(Label.label("Database")).stream().collect(toList());
		assertThat(databaseList).hasSize(1);

		Node databaseNode = databaseList.stream().findFirst().orElse(null);
		assertThat(databaseNode.getAllProperties())
				.containsAllEntriesOf(newHashMap("databaseType", MOCKED_DATABASE_NAME, "name", "testDatabase"));

		//Test database relationships
		Optional<Node> databaseState = neo4jVersionerCore.findStateNode(databaseNode.getId());

		assertThat(databaseState).isPresent();
		assertThat(databaseState.get().getRelationships(RelationshipType.withName("HAS_SCHEMA")))
				.hasSize(1)
				.anySatisfy(relationship -> assertThat(relationship.getStartNode()).isEqualTo(databaseState.get()));

		//Test schema structure
		List<Node> schemaList = neo4j.getGraphDatabaseService().findNodes(Label.label("Schema")).stream().collect(toList());
		assertThat(schemaList)
				.hasSize(1)
				.allSatisfy(schema -> assertThat(schema.getAllProperties())
						.containsAllEntriesOf(newHashMap("name", "testSchema")));

		//Test schema relationships
		Optional<Node> schemaState = neo4jVersionerCore.findStateNode(schemaList.get(0).getId());

		assertThat(schemaState)
				.isPresent()
				.satisfies(schemaNode -> assertThat(schemaNode.get().getRelationships(RelationshipType.withName("HAS_TABLE")))
						.hasSize(2)
						.allSatisfy(relationship -> assertThat(relationship.getStartNode()).isEqualTo(schemaNode.get())));

		//Test tables structure
		List<Node> tableList = neo4j.getGraphDatabaseService().findNodes(Label.label("Table")).stream().collect(toList());
		assertThat(tableList)
				.hasSize(2)
				.anySatisfy(table -> assertThat(table.getAllProperties())
						.containsAllEntriesOf(newHashMap("name", "testTable1")))
				.anySatisfy(table -> assertThat(table.getAllProperties())
						.containsAllEntriesOf(newHashMap("name", "testTable2")));

		//Test tables relationships
		List<Optional<Node>> tableStateList = tableList.stream().map(table -> neo4jVersionerCore.findStateNode(table.getId())).collect(toList());
		assertThat(tableStateList)
				.anySatisfy(tableStateNodeOpt ->
						assertThat(tableStateNodeOpt)
						.isPresent()
						.satisfies(tableStateNode ->
								assertThat(tableStateNode.get().getAllProperties()).hasSize(2).containsAllEntriesOf(
								newHashMap("testTable2Column1", new String[] { "PRIMARY KEY", "NOT NULL" }, "testTable2Column2",
										new String[] { "NOT NULL" }))))
				.anySatisfy(tableStateNodeOpt ->
						assertThat(tableStateNodeOpt)
						.isPresent()
						.satisfies(tableStateNode ->
								assertThat(tableStateNode.get().getAllProperties()).hasSize(1).containsAllEntriesOf(
										newHashMap("testTable1Column1", new String[] { "PRIMARY KEY", "NOT NULL" }))));

		//Test foreign keys
		List<Relationship> foreignKeysList = tableStateList.stream()
				.map(tableStateOpt -> tableStateOpt.get().getRelationships(Direction.OUTGOING, RelationshipType.withName("RELATION")))
				.flatMap(list -> StreamSupport.stream(list.spliterator(), false))
				.collect(toList());

		assertThat(foreignKeysList)
				.hasSize(1)
				.anySatisfy(foreignKey -> {
					assertThat(foreignKey.getStartNode().hasProperty("testTable2Column2")).isTrue();
					assertThat(foreignKey.getAllProperties()).containsAllEntriesOf(
							newHashMap("constraint", "testConstraintName", "source_column", "testTable2Column2", "destination_column", "testTable1Column1")
					);
					assertThat(foreignKey.getEndNode().hasProperty("testTable1Column1")).isTrue();
				});
	}
}