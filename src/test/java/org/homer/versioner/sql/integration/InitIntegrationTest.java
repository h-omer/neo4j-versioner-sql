package org.homer.versioner.sql.integration;

import org.homer.versioner.sql.importers.DatabaseImporter;
import org.homer.versioner.sql.importers.DatabaseImporterFactory;
import org.homer.versioner.sql.importers.PostgresDatabaseImporter;
import org.homer.versioner.sql.persistence.Neo4jVersionerCore;
import org.homer.versioner.sql.procedure.Init;
import org.junit.*;
import org.neo4j.harness.junit.Neo4jRule;
import org.neo4j.logging.BufferingLog;
import org.parboiled.common.Tuple2;

import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.homer.versioner.sql.utils.Utils.newArrayList;
import static org.homer.versioner.sql.utils.Utils.newHashMap;
import static org.mockito.Mockito.*;
import static org.homer.versioner.sql.integration.TestData.*;

public class InitIntegrationTest {

	@Rule
	public Neo4jRule neo4j = new Neo4jRule().withProcedure(Init.class);

	private Neo4jVersionerCore neo4jVersionerCore;

	private static List<DatabaseImporter> databaseList = newArrayList();

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

		initMocks(mockedDatabaseImporter,
				FULLDATABASE.DATABASE(),
				FULLDATABASE.SCHEMAS(),
				FULLDATABASE.TABLES(),
				FULLDATABASE.TABLES_COLUMNS(),
				FULLDATABASE.FOREIGN_KEYS());

		neo4j.getGraphDatabaseService().execute("CALL sql.versioner.init('" + MOCKED_DATABASE_NAME + "', '', 0, '', '', '')");


		//Test database
		assertDatabaseNode(neo4j.getGraphDatabaseService(), neo4jVersionerCore,
				newHashMap("databaseType", MOCKED_DATABASE_NAME, "name", "testDatabase"), 1);


		//Test schema
		assertSchemaNode(neo4j.getGraphDatabaseService(), neo4jVersionerCore,
				newHashMap("name", "testSchema"), 2);

		//Test table
		assertTables(neo4j.getGraphDatabaseService(), neo4jVersionerCore,
				newHashMap("testTable1", newHashMap("testTable1Column1", new String[]{"PRIMARY KEY", "NOT NULL"}),
						"testTable2", newHashMap("testTable2Column1", new String[]{"PRIMARY KEY", "NOT NULL"},
								"testTable2Column2", new String[]{"NOT NULL"})));

		//Test foreign keys
		assertForeignKeys(neo4j.getGraphDatabaseService(), neo4jVersionerCore,
				newHashMap(
						new Tuple2<>("testTable2Column2", "testTable1Column1"), newHashMap("constraint", "testConstraintName", "source_column", "testTable2Column2", "destination_column", "testTable1Column1")
						));
	}
}