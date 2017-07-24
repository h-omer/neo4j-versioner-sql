package org.homer.versioner.sql.persistence;

import lombok.AllArgsConstructor;
import org.homer.versioner.sql.entities.Persisted;
import org.homer.versioner.sql.entities.Versioned;
import org.homer.versioner.sql.exceptions.DatabasePersistenceException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.logging.Log;

import java.util.Optional;
import java.util.stream.Stream;

@AllArgsConstructor
public class Neo4jVersionerCore {

	private GraphDatabaseService graphDb;
	private Log log;

	public Optional<Node> findStateNode(Persisted entity) {

		return new org.homer.versioner.core.builders.GetBuilder().build().flatMap(get ->
				get.getCurrentState(graphDb.getNodeById(entity.getNodeId())).findFirst())
				.map(nodeOutput -> nodeOutput.node);
	}

	public Node createVersionedNode(Versioned entity) {
		return new org.homer.versioner.core.builders.InitBuilder().withDb(graphDb).withLog(log).build()
				.map(init -> init.init(entity.getLabel(), entity.getAttributes(), entity.getProperties(), "", 0L))
				.flatMap(Stream::findFirst)
				.map(node -> node.node)
				.orElseThrow(() -> new DatabasePersistenceException("Cannot persist entity " + entity));
	}

	public void updateVersionedNode(Long entityNodeId, Versioned entity) {
		new org.homer.versioner.core.builders.UpdateBuilder().withDb(graphDb).withLog(log).build()
				.map(update -> update.update(graphDb.getNodeById(entityNodeId), entity.getProperties(), "", 0L));
	}
}
