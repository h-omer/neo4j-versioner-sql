package org.homer.versioner.sql.entities;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.List;

@AllArgsConstructor
@Getter
public class TableColumn {

	private String       name;
	private List<String> attributes;

	public void addAttribute(String attribute) {

		attributes.add(attribute);
	}

	public String[] getAttributesAsArray() {
		return attributes.toArray(new String[attributes.size()]);
	}
}
