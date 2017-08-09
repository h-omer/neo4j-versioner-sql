package org.homer.versioner.sql.model.structure;

import lombok.*;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

@Getter
@ToString
@Builder
@AllArgsConstructor
public class ForeignKey {

	private final String constraintName;
	private String sourceTableName;
	private String sourceSchemaName;
	private final String sourceColumnName;
	private String destinationTableName;
	private String destinationSchemaName;
	private final String destinationColumnName;

	@Setter
	private Long sourceTableId;
	@Setter
	private Long destinationTableId;

	public ForeignKey(ResultSet rs) throws SQLException {
		constraintName = rs.getString(1);
		sourceTableName = rs.getString(2);
		sourceSchemaName = rs.getString(3);
		sourceColumnName = rs.getString(4);
		destinationTableName = rs.getString(5);
		destinationSchemaName = rs.getString(6);
		destinationColumnName = rs.getString(7);
	}

	public ForeignKey(Relationship relationship, Optional<Node> sourceSchema, Optional<Node> sourceTable, Optional<Node> destinationSchema, Optional<Node> destinationTable) {

		constraintName = (String) relationship.getProperty("constraint");
		sourceTableId = relationship.getStartNodeId();
		destinationTableId = relationship.getEndNodeId();
		sourceColumnName = (String) relationship.getProperty("source_column");
		destinationColumnName = (String) relationship.getProperty("destination_column");
		sourceSchemaName = (String) sourceSchema.map(a -> a.getProperty("name")).orElse(null);
		destinationSchemaName = (String) destinationSchema.map(a -> a.getProperty("name")).orElse(null);
		sourceTableName = (String) sourceTable.map(a -> a.getProperty("name")).orElse(null);
		destinationTableName = (String) destinationTable.map(a -> a.getProperty("name")).orElse(null);
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

	public boolean equals(Object obj) {
		if(!(obj instanceof ForeignKey)){
			return false;
		}

		ForeignKey foreignKey = (ForeignKey) obj;

		return StringUtils.equals(this.getConstraintName(), foreignKey.getConstraintName())
				&& StringUtils.equals(this.sourceColumnName, foreignKey.getSourceColumnName())
				&& StringUtils.equals(this.destinationColumnName, foreignKey.getDestinationColumnName())
				&& StringUtils.equals(this.sourceTableName, foreignKey.getSourceTableName())
				&& StringUtils.equals(this.destinationTableName, foreignKey.getDestinationTableName())
				&& StringUtils.equals(this.sourceSchemaName, foreignKey.getSourceSchemaName())
				&& StringUtils.equals(this.destinationSchemaName, foreignKey.getDestinationSchemaName());
	}
}
