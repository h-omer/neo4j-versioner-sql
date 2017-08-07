package org.homer.versioner.sql.model.action;

import org.homer.versioner.sql.model.structure.Database;
import org.homer.versioner.sql.model.structure.Schema;
import org.homer.versioner.sql.model.structure.Table;

import java.util.Optional;

public class SchemaAction extends Action<Schema, Database>{

	protected SchemaAction(ActionType actionType, Schema entity, Optional<Database> parentEntity) {
		super(actionType, entity, parentEntity);
	}

	public static SchemaAction create(Schema schema, Optional<Database> database) {
		return new SchemaAction(ActionType.CREATE, schema, database);
	}

	public static SchemaAction update(Schema schema, Optional<Database> database) {
		return new SchemaAction(ActionType.UPDATE, schema, database);
	}

	public static SchemaAction delete(Schema schema, Optional<Database> database) {
		return new SchemaAction(ActionType.DELETE, schema, database);
	}

	public static SchemaAction noAction(Schema schema) {
		return new SchemaAction(ActionType.NO_ACTION, schema, Optional.empty());
	}
}
