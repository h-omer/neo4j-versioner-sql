package org.homer.versioner.sql.entities;

import lombok.Getter;
import lombok.ToString;
import org.homer.versioner.core.output.NodeOutput;
import org.neo4j.graphdb.Node;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.homer.versioner.sql.utils.Utils.newArrayList;

@Getter
@ToString
public class TableColumn {

	private String       name;
	private List<String> attributes;

	public TableColumn(ResultSet rs) throws SQLException {

		this.name = rs.getString(1);
		attributes = newArrayList(rs.getString(2));

		if (rs.getBoolean(3)) {
			attributes.add("NOT NULL");
		}
	}

	public TableColumn(String name, Object attributes) {

		this.name = name;
		this.attributes = Arrays.asList((String[]) attributes);
	}

	String[] getAttributesAsArray() {
		return attributes.toArray(new String[attributes.size()]);
	}

	public static List<TableColumn> build(Node stateNode) {

		return stateNode.getAllProperties().entrySet().stream()
				.map(property -> new TableColumn(property.getKey(), property.getValue()))
				.collect(Collectors.toList());
	}
}
