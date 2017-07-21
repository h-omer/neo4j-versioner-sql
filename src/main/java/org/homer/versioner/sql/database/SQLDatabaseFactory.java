package org.homer.versioner.sql.database;

import org.homer.versioner.sql.exception.DatabaseNotFoundException;

import java.util.List;

import static java.util.Arrays.asList;

public class SQLDatabaseFactory {

	private static List<SQLDatabase> REGISTERED_DATABASES;

	static {
		REGISTERED_DATABASES = asList(new PostgresSQLDatabase());
	}

	public static SQLDatabase getSQLDatabase(String name) {

		return REGISTERED_DATABASES.stream()
				.filter((registeredDatabase) -> registeredDatabase.getName().equals(name))
				.findFirst()
				.orElseThrow(() -> new DatabaseNotFoundException(name));
	}
}
