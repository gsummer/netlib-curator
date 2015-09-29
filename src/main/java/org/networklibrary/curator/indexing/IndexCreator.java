package org.networklibrary.curator.indexing;

import java.util.List;
import java.util.logging.Logger;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.schema.Schema;
import org.networklibrary.core.config.Dictionary;
import org.networklibrary.curator.Task;

public class IndexCreator implements Task {

	protected static final Logger log = Logger.getLogger(IndexCreator.class.getName());

	protected GraphDatabaseService g = null;

	protected Dictionary dict;
	
	protected String label = null;
	protected String property = null;
	
	@Override
	public void setGraph(GraphDatabaseService g) {
		this.g = g;
		
	}

	@Override
	public void execute() {
		if(label != null && !label.isEmpty() && property != null && !property.isEmpty()){
			Schema schema = g.schema();
			schema.indexFor(DynamicLabel.label(label)).on(property).create();
		}
		
	}

	@Override
	public void setExtraParameters(List<String> extras) {
		if(extras != null){
			log.info("processing extra parameters: " + extras.toString());

			for(String extra : extras){
				String values[] = extra.split("=",-1);

				switch(values[0]) {
				
				case "label":
					label = values[1];
					break;
				
				case "property":
					property = values[1];
					break;
				}

			}

			log.info("using label: " + label);
			log.info("using property:" + property);
		}
		
	}

	@Override
	public void setDictionary(Dictionary dict) {
		this.dict = dict;
		
	}

}
