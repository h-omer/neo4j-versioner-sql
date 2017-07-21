package org.homer.versioner.sql.entities;

import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

import static org.homer.versioner.sql.utils.Utils.newArrayList;

@Getter
public class Schema {

	private Long 		nodeId;
	private String      name;

	private List<Table> tables;

	public Schema(ResultSet rs) throws SQLException {

		this.name = rs.getString(1);
		tables = newArrayList();
	}

	public void addTable(Table table) {
		tables.add(table);
	}

	Optional<Table> findTable(String sourceTableName) {

		return tables.stream()
				.filter(table -> StringUtils.equals(table.getName(), sourceTableName))
				.findFirst();
	}

	public void setNodeId(Long nodeId) {
		this.nodeId = nodeId;
	}
}
