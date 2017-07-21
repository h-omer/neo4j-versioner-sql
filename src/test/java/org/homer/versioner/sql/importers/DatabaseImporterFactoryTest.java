package org.homer.versioner.sql.importers;

import org.junit.Assert;
import org.junit.Test;

public class DatabaseImporterFactoryTest {
    @Test
    public void shouldReturnAPostgresInstance() {
        String brandName = "postgres";
        DatabaseImporter databaseImporter = DatabaseImporterFactory.getSQLDatabase(brandName);

        Assert.assertEquals(brandName, databaseImporter.getName());
    }
}
