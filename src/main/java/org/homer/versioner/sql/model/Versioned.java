package org.homer.versioner.sql.model;

import org.homer.versioner.sql.model.structure.Persisted;

import java.util.Map;

public interface Versioned extends Persisted {

	Map<String, Object> getAttributes();
	Map<String, Object> getProperties();
	String getLabel();
}
