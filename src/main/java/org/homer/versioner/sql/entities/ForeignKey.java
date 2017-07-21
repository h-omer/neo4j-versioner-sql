package org.homer.versioner.sql.entities;

import lombok.Getter;
import org.neo4j.graphdb.Relationship;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

@Getter
public class ForeignKey {

	private final String constraintName;
	private final String sourceTableName;
	private final String sourceSchemaName;
	private final String sourceColumnName;
	private final String destinationTableName;
	private final String destinationSchemaName;
	private final String destinationColumnName;

	private Optional<TableNode> sourceTable;
	private Optional<TableNode> destinationTable;

	public ForeignKey(ResultSet rs, DatabaseNode database) throws SQLException {
		constraintName = rs.getString(1);
		sourceTableName = rs.getString(2);
		sourceSchemaName = rs.getString(3);
		sourceColumnName = rs.getString(4);
		destinationTableName = rs.getString(5);
		destinationSchemaName = rs.getString(6);
		destinationColumnName = rs.getString(7);

		sourceTable = findTable(database, sourceSchemaName, sourceTableName);
		destinationTable = findTable(database, destinationSchemaName, destinationTableName);
	}

	private Optional<TableNode> findTable(DatabaseNode database, String schemaName, String tableName) {

		return database.findSchema(schemaName)
				.map(schema -> schema.findTable(tableName))
				.orElse(Optional.empty());
	}

	public void setRelationshipProperties(Relationship relationship) {

		relationship.setProperty("constraint", constraintName);
		relationship.setProperty("source_column", sourceColumnName);
		relationship.setProperty("destination_column", destinationColumnName);
	}
}
