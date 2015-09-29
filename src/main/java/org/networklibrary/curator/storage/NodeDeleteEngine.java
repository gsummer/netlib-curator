package org.networklibrary.curator.storage;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.networklibrary.core.config.ConfigManager;
import org.networklibrary.core.storage.MultiTxStrategy;
import org.networklibrary.core.types.NodeDelete;

public class NodeDeleteEngine extends MultiTxStrategy<NodeDelete> {

	public NodeDeleteEngine(GraphDatabaseService graph, ConfigManager confMgr) {
		super(graph, confMgr);
	}

	@Override
	protected void doStore(NodeDelete curr) {

		Node n = null;

		try {
			n = getGraph().getNodeById(curr.getId());
		} catch (NotFoundException e){
			n = null;
		}

		if(n != null){
			if(curr.withEdges()){
//				Set<Relationship> rels = new HashSet<Relationship>();
				for(Relationship r : n.getRelationships(Direction.BOTH)){
//					rels.add(e)
					r.delete();
				}
			}

			if(n.getDegree() == 0) {
				n.delete();
			}
		}

	}

}
