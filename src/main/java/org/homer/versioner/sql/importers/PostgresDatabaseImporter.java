package org.homer.versioner.sql.importers;

public class PostgresDatabaseImporter extends DatabaseImporter {

	@Override
	public String getName() {
		return "postgres";
	}

	@Override
	protected String getConnectionUrl() {
		return "jdbc:postgresql://%s:%s/%s";
	}

	@Override
	protected String getSchemaQuery() {
		return "SELECT DISTINCT table_schema FROM information_schema.tables WHERE table_schema NOT IN ('pg_catalog', 'information_schema')";
	}

	@Override
	protected String getTablesQuery() {
		return "SELECT table_name FROM information_schema.tables WHERE table_schema = '%s'";
	}

	@Override
	protected String getColumnsQuery() {
		return "SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_schema = '%s' AND table_name = '%s'";
	}

	@Override
	public String getForeignKeysQuery() {
		return "SELECT DISTINCT tc.constraint_name, tc.table_name, tc.constraint_schema, kcu.column_name, ccu.table_name AS foreign_table_name, ccu.table_schema, ccu.column_name AS foreign_column_name FROM information_schema.table_constraints AS tc JOIN information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name JOIN information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name WHERE constraint_type = 'FOREIGN KEY'";
	}
}
