package org.homer.versioner.sql.entities;

import lombok.Getter;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static org.homer.versioner.sql.utils.Utils.newArrayList;

@Getter
public class TableColumn {

	private String       name;
	private List<String> attributes;

	public TableColumn(ResultSet rs) throws SQLException {

		this.name = rs.getString(1);
		attributes = newArrayList(rs.getString(2));

		if (rs.getBoolean(3)) {
			attributes.add("NOT NULL");
		}
	}

	public String[] getAttributesAsArray() {
		return attributes.toArray(new String[attributes.size()]);
	}
}
