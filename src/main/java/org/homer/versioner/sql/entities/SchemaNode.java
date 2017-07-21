package org.homer.versioner.sql.entities;

import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

import static org.homer.versioner.sql.utils.Utils.newArrayList;

@Getter
public class SchemaNode {

	private Node        node;
	private String      name;
	private List<TableNode> tables;

	public SchemaNode(Node node, ResultSet rs) throws SQLException {

		this.node = node;
		this.name = rs.getString(1);

		this.node.addLabel(Label.label("Schema"));
		this.node.setProperty("name", name);

		tables = newArrayList();
	}

	public void addTable(TableNode table) {

		tables.add(table);
		node.createRelationshipTo(table.getNode(), RelationshipType.withName("HAS_TABLE"));
	}

	public Optional<TableNode> findTable(String sourceTableName) {
		return tables.stream()
				.filter(table -> StringUtils.equals(table.getName(), sourceTableName))
				.findFirst();
	}
}
