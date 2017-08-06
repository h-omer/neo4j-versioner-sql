package org.homer.versioner.sql.model.structure;

import org.homer.versioner.sql.model.Action;
import org.neo4j.logging.Log;

import java.util.Optional;

public class DiffManager {

	public static Action<Table, Schema> getDiffs(Table table, Schema schema, Database existingDatabase, Log log) {

		Optional<Schema> existingSchemaOpt = existingDatabase.findSchema(schema.getName());
		Optional<Table> existingTableOpt = existingSchemaOpt.flatMap(existingSchema -> existingSchema.findTable(table.getName()));

		if (!existingTableOpt.isPresent()) {
			return Action.create(table, existingSchemaOpt);
		} else if (!existingTableOpt.get().equals(table)) {
			return Action.update(table, existingSchemaOpt);
		}

		return Action.noAction(table);
	}

	public static Action<Schema, Database> getDiffs(Schema schema, Database existingDatabase) {

		Optional<Schema> existingSchemaOpt = existingDatabase.findSchema(schema.getName());

		if (!existingSchemaOpt.isPresent()) {
			return Action.create(schema, Optional.of(existingDatabase));
		} else if (!existingSchemaOpt.get().equals(schema)) {
			return Action.update(schema, Optional.of(existingDatabase));
		}

		return Action.noAction(schema);
	}
}
