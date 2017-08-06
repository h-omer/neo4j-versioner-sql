package org.homer.versioner.sql.integration;

import org.homer.versioner.sql.persistence.Neo4jVersionerCore;
import org.homer.versioner.sql.procedure.Init;
import org.homer.versioner.sql.procedure.Reload;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.harness.junit.Neo4jRule;
import org.neo4j.logging.BufferingLog;
import org.neo4j.logging.Log;

public class ReloadIntegrationTest {
	@Rule public Neo4jRule neo4j = new Neo4jRule()

			// This is the function we want to test
			.withProcedure(Init.class)
			.withProcedure(Reload.class);

	@Rule public Log log = new BufferingLog();

	private Neo4jVersionerCore neo4jVersionerCore;

	@Before
	public void setUp() {
		neo4jVersionerCore = new Neo4jVersionerCore(neo4j.getGraphDatabaseService(), log);
	}

	@Test
	public void shouldImportCorrectlyNewNodes() {


	}
}
