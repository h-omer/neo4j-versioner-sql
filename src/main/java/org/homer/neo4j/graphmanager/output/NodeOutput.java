package org.homer.neo4j.graphmanager.output;

import org.neo4j.graphdb.Node;

import java.util.Optional;

public class NodeOutput {
    public Node node;

    public NodeOutput(Node node) {
        this.node = node;
    }

}
