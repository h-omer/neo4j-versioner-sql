package org.homer.versioner.sql.database;

import org.junit.Assert;
import org.junit.Test;

public class SQLDatabaseFactoryTest {
    @Test
    public void shouldReturnAPostgresInstance() {
        String brandName = "postgres";
        SQLDatabase sqlDatabase = SQLDatabaseFactory.getSQLDatabase(brandName);

        Assert.assertEquals(brandName, sqlDatabase.getName());
    }
}
