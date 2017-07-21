package org.homer.versioner.sql.entities;

import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;

import java.util.List;
import java.util.Optional;

import static org.homer.versioner.sql.utils.Utils.newArrayList;

@Getter
public class DatabaseNode {

	private Node             node;
	private List<SchemaNode> schemas;

	public DatabaseNode(Node node, String name){

		this.node = node;
		node.addLabel(Label.label("Database"));
		node.setProperty("name", name);

		schemas = newArrayList();
	}

	public void addSchema(SchemaNode schema) {

		schemas.add(schema);
		node.createRelationshipTo(schema.getNode(), RelationshipType.withName("HAS_SCHEMA"));
	}

	public Optional<SchemaNode> findSchema(String sourceSchemaName) {
		return schemas.stream()
				.filter(schema -> StringUtils.equals(schema.getName(), sourceSchemaName))
				.findFirst();
	}
}
