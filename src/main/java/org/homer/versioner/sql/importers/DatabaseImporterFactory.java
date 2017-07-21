package org.homer.versioner.sql.importers;

import org.homer.versioner.sql.exceptions.DatabaseNotFoundException;

import java.util.List;

import static java.util.Arrays.asList;

public class DatabaseImporterFactory {

	private static List<DatabaseImporter> REGISTERED_DATABASES;

	static {
		REGISTERED_DATABASES = asList(new PostgresDatabaseImporter());
	}

	public static DatabaseImporter getSQLDatabase(String name) {

		return REGISTERED_DATABASES.stream()
				.filter((registeredDatabase) -> registeredDatabase.getName().equals(name))
				.findFirst()
				.orElseThrow(() -> new DatabaseNotFoundException(name));
	}
}
