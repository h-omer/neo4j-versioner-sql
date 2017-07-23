package org.homer.versioner.sql.procedure;

import org.homer.versioner.core.output.NodeOutput;
import org.homer.versioner.sql.entities.*;
import org.homer.versioner.sql.importers.DatabaseImporter;
import org.homer.versioner.sql.importers.DatabaseImporterFactory;
import org.homer.versioner.sql.persistence.Neo4jLoader;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.List;
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

		Neo4jLoader neo4jLoader = new Neo4jLoader(db);

		Database database = neo4jLoader.loadDatabase();

		DatabaseImporter databaseImporter = DatabaseImporterFactory.getSQLDatabase(database.getDatabaseType());
		databaseImporter.connect(hostname, port, database.getName(), username, password);

		neo4jLoader.loadSchemas(database).forEach(schema -> {
			database.addSchema(schema);
			neo4jLoader.loadTables(schema).forEach(schema::addTable);
		});

		List<Schema> schemas = databaseImporter.getSchemas();
		schemas.forEach(schema -> {

		});

		//TODO reuse loader of method init
		Database existingDatabase = databaseImporter.getDatabase();

		List<Schema> existingSchemas = databaseImporter.getSchemas();
		existingSchemas.forEach(schema -> {

			existingDatabase.addSchema(schema);

			List<Table> tables = databaseImporter.getTables(schema);
			tables.forEach(table -> {

				schema.addTable(table);

				List<TableColumn> columns = databaseImporter.getColumns(schema, table);
				columns.forEach(table::addColumn);
			});
		});

		List<ForeignKey> foreignKeys = databaseImporter.getForeignKeys();
		foreignKeys.forEach(foreignKey ->
				foreignKey.getSourceTable(database).ifPresent(sourceTable -> sourceTable.addForeignKey(foreignKey))
		);

		//TODO manage diff of states and add new states

		return Stream.of(new NodeOutput(db.getNodeById(database.getNodeId())));
	}
}
