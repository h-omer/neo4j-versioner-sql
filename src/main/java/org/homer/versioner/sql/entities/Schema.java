package org.homer.versioner.sql.entities;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.Node;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.homer.versioner.sql.utils.Utils.newArrayList;
import static org.homer.versioner.sql.utils.Utils.newHashMap;

@Getter
public class Schema implements Versioned, Persisted {

	@Setter
	private Long 		nodeId;
	private String      name;

	private List<Table> tables = newArrayList();
	private Map<String, Object> attributes;

	public Schema(ResultSet rs) throws SQLException {

		this.name = rs.getString(1);
	}

	public Schema(Node node) {

		this.name = (String) node.getProperty("name");
		this.nodeId = node.getId();
	}

	public void addTable(Table table) {
		tables.add(table);
	}

	Optional<Table> findTable(String sourceTableName) {

		return tables.stream()
				.filter(table -> StringUtils.equals(table.getName(), sourceTableName))
				.findFirst();
	}

	public Map<String, Object> getAttributes() {
		return newHashMap("name", name);
	}

	@Override public Map<String, Object> getProperties() {
		return newHashMap();
	}

	@Override public String getLabel() {
		return "Schema";
	}
}
