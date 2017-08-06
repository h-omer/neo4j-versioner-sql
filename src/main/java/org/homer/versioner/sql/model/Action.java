package org.homer.versioner.sql.model;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import org.homer.versioner.sql.model.structure.Persisted;

import java.util.Objects;
import java.util.Optional;

@Getter
@AllArgsConstructor(access = AccessLevel.PROTECTED)
@ToString
public class Action <T extends Versioned , E extends Versioned> {

	private ActionType  actionType;
	private T           entity;
	private Optional<E> parentEntity;

	public static <T extends Versioned, E extends Versioned> Action<T, E> create(T entity, Optional<E> parentEntity) {
		return new Action<>(ActionType.CREATE, entity, parentEntity);
	}

	public static <T extends Versioned, E extends Versioned> Action<T, E> update(T entity, Optional<E> parentEntity) {
		return new Action<>(ActionType.UPDATE, entity, parentEntity);
	}

	public static <T extends Versioned, E extends Versioned> Action<T, E> delete(T entity, Optional<E> parentEntity) {
		return new Action<>(ActionType.DELETE, entity, parentEntity);
	}

	public static <T extends Versioned, E extends Versioned> Action<T, E> noAction(T entity) {
		return new Action<>(ActionType.NO_ACTION, entity, Optional.empty());
	}

	public boolean is(ActionType actionType) {
		return Objects.equals(this.actionType, actionType);
	}
}
