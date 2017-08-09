package org.homer.versioner.sql.model.structure;

import org.homer.versioner.sql.model.action.SchemaAction;
import org.homer.versioner.sql.model.action.TableAction;
import org.neo4j.logging.Log;

import java.util.Optional;

public class DiffManager {

	public static TableAction getDiffs(Table table, Schema schema, Database existingDatabase, Log log) {

		Optional<Schema> existingSchemaOpt = existingDatabase.findSchema(schema.getName());
		Optional<Table> existingTableOpt = existingSchemaOpt.flatMap(existingSchema -> existingSchema.findTable(table.getName()));

		if (!existingTableOpt.isPresent()) {
			return TableAction.create(enrich(table, existingDatabase), existingSchemaOpt);
		} else if (!existingTableOpt.get().equals(table)) {
			return TableAction.update(enrich(table, existingDatabase), existingSchemaOpt);
		}

		return TableAction.noAction(table);
	}

	public static SchemaAction getDiffs(Schema schema, Database existingDatabase) {

		Optional<Schema> existingSchemaOpt = existingDatabase.findSchema(schema.getName());

		if (!existingSchemaOpt.isPresent()) {
			return SchemaAction.create(schema, Optional.of(existingDatabase));
		} else if (!existingSchemaOpt.get().equals(schema)) {
			return SchemaAction.update(schema, Optional.of(existingDatabase));
		}

		return SchemaAction.noAction(schema);
	}

	private static Table enrich(Table table, Database existingDatabase) {

		table.getForeignKeys().forEach(foreignKey -> {
			Optional<Table> existingDestinationTableOpt = existingDatabase.findSchema(foreignKey.getDestinationSchemaName())
					.flatMap(existingSchema -> existingSchema.findTable(foreignKey.getDestinationTableName()));

			existingDestinationTableOpt.ifPresent(existingTable -> foreignKey.setDestinationTableId(existingTable.getNodeId()));
		});

		return table;
	}
}
