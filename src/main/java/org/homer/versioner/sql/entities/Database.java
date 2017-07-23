package org.homer.versioner.sql.entities;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.Node;

import java.util.List;
import java.util.Optional;

import static org.homer.versioner.sql.utils.Utils.newArrayList;

@Getter
public class Database {

	@Setter
	private Long 			nodeId;
	private String 			name;
	private String 			databaseType;

	private List<Schema> 	schemas = newArrayList();

	public Database(String name, String databaseType){

		this.name = name;
		this.databaseType = databaseType;
	}

	public Database(Node databaseNode) {

		nodeId = databaseNode.getId();
		name = (String) databaseNode.getProperty("name");
		databaseType = (String) databaseNode.getProperty("databaseType");
	}

	public Database(Database database) {

		nodeId = database.nodeId;
		name = database.name;
		databaseType = database.databaseType;
	}

	public void addSchema(Schema schema) {
		schemas.add(schema);
	}

	Optional<Schema> findSchema(String sourceSchemaName) {

		return schemas.stream()
				.filter(schema -> StringUtils.equals(schema.getName(), sourceSchemaName))
				.findFirst();
	}
}
