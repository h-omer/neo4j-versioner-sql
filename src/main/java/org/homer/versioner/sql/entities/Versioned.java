package org.homer.versioner.sql.entities;

import java.util.Map;

public interface Versioned {

	Map<String, Object> getAttributes();
	Map<String, Object> getProperties();
	String getLabel();
}
