package org.networklibrary.curator;

import java.util.List;

import org.neo4j.graphdb.GraphDatabaseService;
import org.networklibrary.core.config.Dictionary;

public interface Task {
	
	public void setGraph(GraphDatabaseService g);
	
	public void execute();

	public void setExtraParameters(List<String> extras);

	public void setDictionary(Dictionary dict);
}
