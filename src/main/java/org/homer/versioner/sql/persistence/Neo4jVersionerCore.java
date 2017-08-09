package org.homer.versioner.sql.persistence;

import lombok.AllArgsConstructor;
import org.homer.versioner.sql.model.structure.Persisted;
import org.homer.versioner.sql.model.Versioned;
import org.homer.versioner.sql.exceptions.DatabasePersistenceException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.logging.Log;

import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import static org.homer.versioner.sql.utils.Utils.newHashMap;

@AllArgsConstructor
public class Neo4jVersionerCore {

	private GraphDatabaseService graphDb;
	private Log log;

	public Optional<Node> findStateNode(Persisted entity) {
		return findStateNode(entity.getNodeId());
	}

	public Optional<Node> findStateNode(Long nodeId) {
		return new org.homer.versioner.core.builders.GetBuilder().build().flatMap(get ->
				get.getCurrentState(graphDb.getNodeById(nodeId)).findFirst())
				.map(nodeOutput -> nodeOutput.node);
	}

	public Node createVersionedNode(Versioned entity) {
		return new org.homer.versioner.core.builders.InitBuilder().withDb(graphDb).withLog(log).build()
				.map(init -> init.init(entity.getLabel(), entity.getAttributes(), entity.getProperties(), "", 0L))
				.flatMap(Stream::findFirst)
				.map(node -> node.node)
				.orElseThrow(() -> new DatabasePersistenceException("Cannot persist entity " + entity));
	}

	public void updateVersionedNode(Long entityNodeId, Versioned newState) {
		new org.homer.versioner.core.builders.UpdateBuilder().withDb(graphDb).withLog(log).build()
				.map(update -> update.update(graphDb.getNodeById(entityNodeId), Objects.nonNull(newState) ? newState.getProperties() : newHashMap(), "", 0L));
	}
}
