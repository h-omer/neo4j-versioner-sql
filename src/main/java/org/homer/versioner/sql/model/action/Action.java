package org.homer.versioner.sql.model.action;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import org.homer.versioner.sql.model.Versioned;

import java.util.Objects;
import java.util.Optional;

@Getter
@AllArgsConstructor(access = AccessLevel.PROTECTED)
@ToString
public abstract class Action <T extends Versioned, E extends Versioned> {

	private ActionType  actionType;
	private T           entity;
	private Optional<E> parentEntity;

	public boolean is(ActionType actionType) {
		return Objects.equals(this.actionType, actionType);
	}
}
