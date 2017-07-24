package org.homer.versioner.sql.entities;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.neo4j.graphdb.Node;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static org.homer.versioner.sql.utils.Utils.newArrayList;
import static org.homer.versioner.sql.utils.Utils.newHashMap;

@Getter
@ToString
public class Table implements Versioned, Persisted {

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
}
