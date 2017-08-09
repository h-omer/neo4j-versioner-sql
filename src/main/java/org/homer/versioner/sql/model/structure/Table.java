package org.homer.versioner.sql.model.structure;

import lombok.*;
import org.homer.versioner.sql.model.Versioned;
import org.neo4j.graphdb.Node;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static org.homer.versioner.sql.utils.Utils.newArrayList;
import static org.homer.versioner.sql.utils.Utils.newHashMap;
import static org.homer.versioner.sql.utils.Utils.nullSafeEquals;

@Getter
@ToString
@Builder
@AllArgsConstructor
public class Table implements Versioned {

	private Long 			  nodeId;
	private String            name;

	@Setter
	private List<TableColumn> columns = newArrayList();
	@Setter
	private List<ForeignKey>  foreignKeys = newArrayList();

	public Table(ResultSet rs) throws SQLException {

		this.name = rs.getString(1);
	}

	public Table(Node node) {

		this.name = (String) node.getProperty("name");
		this.nodeId = node.getId();
	}

	public void addColumn(TableColumn column) {
	    columns.add(column);
	}

	public Map<String, Object> getAttributes() {
		return newHashMap("name", name);
	}

	public Map<String, Object> getProperties() {

		Map<String, Object> result = newHashMap();
		columns.forEach((column) -> result.put(column.getName(), column.getAttributesAsArray()));
		return result;
	}

	@Override public String getLabel() {
		return "Table";
	}

	public void addForeignKey(ForeignKey foreignKey) {
		foreignKeys.add(foreignKey);
	}

	public void setNodeId(Long nodeId) {
		this.nodeId = nodeId;
	}

	@Override
	public boolean equals(Object obj) {

		if(!(obj instanceof Table)) {
			return false;
		}

		Table table = (Table) obj;

		return nullSafeEquals(this.name, table.name) &&
				table.columns.containsAll(this.columns) &&
				this.columns.containsAll(table.columns);
	}
}
