package org.homer.versioner.sql.database;

public class SQLDatabaseFactory {

    public static SQLDatabase getSQLDatabase(String name) {
        SQLDatabase result = null;

        if (name.equals("postgres")) {
            result = new Postgres();
        }

        return result;
    }
}
