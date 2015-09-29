package org.networklibrary.curator.storage;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.networklibrary.core.config.ConfigManager;
import org.networklibrary.core.storage.MultiTxStrategy;
import org.networklibrary.core.types.EdgeDelete;

public class EdgeDeleteEngine extends MultiTxStrategy<EdgeDelete> {

	public EdgeDeleteEngine(GraphDatabaseService graph, ConfigManager confMgr) {
		super(graph, confMgr);
	}

	@Override
	protected void doStore(EdgeDelete curr) {
		
		Relationship r = null;
		
		try{
			r = getGraph().getRelationshipById(curr.getId());
		} catch (NotFoundException e ){
			r = null;
		}
		
		if(r != null){
			r.delete();
		}
		
	}

}
