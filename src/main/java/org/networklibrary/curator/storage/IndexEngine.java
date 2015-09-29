package org.networklibrary.curator.storage;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.schema.Schema;
import org.networklibrary.core.config.ConfigManager;
import org.networklibrary.core.storage.SingleTxStrategy;
import org.networklibrary.core.types.Index;

public class IndexEngine extends SingleTxStrategy<Index> {

	public IndexEngine(GraphDatabaseService graph, ConfigManager confMgr) {
		super(graph, confMgr);

	}

	@Override
	protected void doStore(Index curr) {
		if(isValid(curr)){
			Schema schema = getGraph().schema();
			schema.indexFor(DynamicLabel.label(curr.getLabel())).on(curr.getProperty()).create();
		}
	}

	protected boolean isValid(Index curr) {
		return	curr.getLabel() != null && !curr.getLabel().isEmpty() && 
				curr.getProperty() != null && !curr.getProperty().isEmpty();
	}

}
