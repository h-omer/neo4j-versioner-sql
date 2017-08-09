package org.homer.versioner.sql.procedure;

import org.homer.versioner.core.output.NodeOutput;
import org.homer.versioner.sql.importers.DatabaseImporter;
import org.homer.versioner.sql.importers.DatabaseImporterFactory;
import org.homer.versioner.sql.model.action.SchemaAction;
import org.homer.versioner.sql.model.action.TableAction;
import org.homer.versioner.sql.model.structure.*;
import org.homer.versioner.sql.persistence.Neo4jLoader;
import org.homer.versioner.sql.persistence.Neo4jPersistence;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.stream.Stream;

public class Reload {

	@Context
	public GraphDatabaseService db;

	@Context
	public Log log;

	@Procedure(value = "sql.versioner.reload", mode = Mode.WRITE)
	@Description("sql.versioner.reload(hostname, port, username, password) - Reload a new Database Version with its schemas, tables and relative columns.")
	public Stream<NodeOutput> reload(
			@Name("hostname") String hostname,
			@Name("port") Long port,
			@Name("username") String username,
			@Name("password") String password) {

		Neo4jLoader neo4jLoader = new Neo4jLoader(db, log);

		Database existingDatabase = neo4jLoader.loadDatabase();

		DatabaseImporter databaseImporter = DatabaseImporterFactory.getSQLDatabase(existingDatabase.getDatabaseType());
		databaseImporter.connect(hostname, port, existingDatabase.getName(), username, password);

		neo4jLoader.loadSchemas(existingDatabase).forEach(schema -> {
			existingDatabase.addSchema(schema);
			neo4jLoader.loadTables(schema).forEach(schema::addTable);
		});

		Database database = Init.loadDatabaseFromDBMS(databaseImporter);

		databaseImporter.disconnect();

		Neo4jPersistence persistence = new Neo4jPersistence(db, log);

		//TODO calculate deleted tables from current DBMS
		//TODO group changes and apply them for each different node once
		//Process database diffs and persist
		database.getSchemas().forEach(schema -> {
			schema.getTables().forEach(table -> {
				TableAction diffTable = DiffManager.getDiffs(table, schema, existingDatabase, log);
				persistence.persist(diffTable);
			});

			SchemaAction diffSchema = DiffManager.getDiffs(schema, existingDatabase);
			persistence.persist(diffSchema);
		});

		return Stream.of(new NodeOutput(db.getNodeById(existingDatabase.getNodeId())));
	}
}
