package org.homer.versioner.sql.importers;

public class MySQLDatabaseImporter extends DatabaseImporter {

	@Override
	public String getName() {
		return "mysql";
	}

	@Override
	protected String getConnectionUrl() {
		return "jdbc:mysql://%s:%s/%s";
	}

	@Override
	protected String getSchemaQuery() {
		return "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME NOT IN ('information_schema', 'mysql', 'sys', 'performance_schema')";
	}

	@Override
	protected String getTablesQuery() {
		return "SELECT TABLE_NAME FROM information_schema.tables WHERE TABLE_SCHEMA = '%s'";
	}

	@Override
	protected String getColumnsQuery() {
		return "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'";
	}

	@Override
	public String getForeignKeysQuery() {
		return "SELECT DISTINCT tc.constraint_name, tc.table_name, tc.constraint_schema, kcu.column_name, kcu.REFERENCED_TABLE_NAME AS foreign_table_name, kcu.table_schema, kcu.REFERENCED_COLUMN_NAME AS foreign_column_name FROM information_schema.table_constraints AS tc JOIN information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name WHERE constraint_type = 'FOREIGN KEY'";
	}
}
