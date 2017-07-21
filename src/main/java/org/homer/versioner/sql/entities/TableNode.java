package org.homer.versioner.sql.entities;

import lombok.Getter;
import lombok.Setter;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static org.homer.versioner.sql.utils.Utils.newArrayList;
import static org.homer.versioner.sql.utils.Utils.newHashMap;

@Getter
public class TableNode {

	@Setter
	private Node              node;
	private String            name;
	private List<TableColumn> columns;
	private List<ForeignKey>  foreignKeys;

	public TableNode(ResultSet rs) throws SQLException {

		this.name = rs.getString(1);
		this.columns = newArrayList();
		this.foreignKeys = newArrayList();
	}

	public Map<String, Object> getAttributes() {

		return newHashMap("name", name);
	}

	public void addColumn(ResultSet columnRs) throws SQLException {

		TableColumn column = new TableColumn(columnRs.getString(1), newArrayList(columnRs.getString(2)));

		if (columnRs.getBoolean(3)) {
			column.addAttribute("NOT NULL");
		}

		columns.add(column);
	}

	public Map<String, Object> getProperties() {

		Map<String, Object> result = newHashMap();
		columns.forEach((column) -> {
			result.put(column.getName(), column.getAttributesAsArray());
		});
		return result;
	}

	public void addForeignKey(ForeignKey foreignKey) {

		foreignKeys.add(foreignKey);

		foreignKey.getDestinationTable().ifPresent(destinationTable -> {
			Relationship relationship = node.createRelationshipTo(destinationTable.getNode(), RelationshipType.withName("RELATION"));
			foreignKey.setRelationshipProperties(relationship);
		});
	}
}
