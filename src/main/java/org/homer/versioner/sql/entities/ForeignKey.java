package org.homer.versioner.sql.entities;

import lombok.Getter;
import lombok.ToString;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

@Getter
@ToString
public class ForeignKey {

	private final String constraintName;
	private final String sourceTableName;
	private final String sourceSchemaName;
	private final String sourceColumnName;
	private final String destinationTableName;
	private final String destinationSchemaName;
	private final String destinationColumnName;

	public ForeignKey(ResultSet rs) throws SQLException {
		constraintName = rs.getString(1);
		sourceTableName = rs.getString(2);
		sourceSchemaName = rs.getString(3);
		sourceColumnName = rs.getString(4);
		destinationTableName = rs.getString(5);
		destinationSchemaName = rs.getString(6);
		destinationColumnName = rs.getString(7);
	}

	private Optional<Table> findTable(Database database, String schema, String table) {

		return database.findSchema(schema)
				.map(foundSchema -> foundSchema.findTable(table))
				.orElse(Optional.empty());
	}

	public Optional<Table> getSourceTable(Database database) {
		return findTable(database, sourceSchemaName, sourceTableName);
	}

	public Optional<Table> getDestinationTable(Database database) {
		return findTable(database, destinationSchemaName, destinationTableName);
	}
}
