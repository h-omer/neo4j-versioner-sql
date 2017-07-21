package org.homer.versioner.sql.entities;

import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Optional;

import static org.homer.versioner.sql.utils.Utils.newArrayList;

@Getter
public class Database {

	private Long 			nodeId;
	private String 			name;

	private List<Schema> 	schemas;

	public Database(String name){

		this.name = name;
		schemas = newArrayList();
	}

	public void addSchema(Schema schema) {
		schemas.add(schema);
	}

	Optional<Schema> findSchema(String sourceSchemaName) {

		return schemas.stream()
				.filter(schema -> StringUtils.equals(schema.getName(), sourceSchemaName))
				.findFirst();
	}

	public void setNodeId(Long nodeId) {
		this.nodeId = nodeId;
	}
}
