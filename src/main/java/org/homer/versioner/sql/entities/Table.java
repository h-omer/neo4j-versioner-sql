package org.homer.versioner.sql.entities;

import lombok.Getter;
import lombok.ToString;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static org.homer.versioner.sql.utils.Utils.newArrayList;
import static org.homer.versioner.sql.utils.Utils.newHashMap;

@Getter
@ToString
public class Table {

	private Long 			  nodeId;
	private String            name;

	private List<TableColumn> columns;
	private List<ForeignKey>  foreignKeys;

	public Table(ResultSet rs) throws SQLException {

		this.name = rs.getString(1);
		this.columns = newArrayList();
		this.foreignKeys = newArrayList();
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

	public void addForeignKey(ForeignKey foreignKey) {
		foreignKeys.add(foreignKey);
	}

	public void setNodeId(Long nodeId) {
		this.nodeId = nodeId;
	}
}
