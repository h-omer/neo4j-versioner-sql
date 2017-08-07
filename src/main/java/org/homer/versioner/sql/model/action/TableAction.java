package org.homer.versioner.sql.model.action;

import org.homer.versioner.sql.model.structure.Schema;
import org.homer.versioner.sql.model.structure.Table;

import java.util.Optional;

public class TableAction extends Action<Table, Schema>{

	protected TableAction(ActionType actionType, Table entity, Optional<Schema> parentEntity) {
		super(actionType, entity, parentEntity);
	}

	public static TableAction create(Table table, Optional<Schema> schema) {
		return new TableAction(ActionType.CREATE, table, schema);
	}

	public static TableAction update(Table table, Optional<Schema> schema) {
		return new TableAction(ActionType.UPDATE, table, schema);
	}

	public static TableAction delete(Table table, Optional<Schema> schema) {
		return new TableAction(ActionType.DELETE, table, schema);
	}

	public static TableAction noAction(Table table) {
		return new TableAction(ActionType.NO_ACTION, table, Optional.empty());
	}
}
