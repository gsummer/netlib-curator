package org.networklibrary.curator.rewiring;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.logging.Logger;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.tooling.GlobalGraphOperations;
import org.networklibrary.core.config.Dictionary;
import org.networklibrary.curator.Task;
import org.networklibrary.curator.heattransfer.MetaHeatTransfer;

public class GraphRewiring implements Task {

	protected static final Logger log = Logger.getLogger(MetaHeatTransfer.class.getName());

	protected GraphDatabaseService g = null;

	protected Set<String> edgeTypes = new HashSet<String>();
	protected Set<String> include = new HashSet<String>();
	protected Set<String> target = new HashSet<String>();

	protected String postfix;
	
	private Dictionary dict;
	
	@Override
	public void setGraph(GraphDatabaseService g) {
		this.g = g;

	}

	@Override
	public void execute() {
		Set<RelationshipType> relTypes = new HashSet<RelationshipType>();
		Set<Label> includeLabels = new HashSet<Label>();
		Set<Label> targetLabels = new HashSet<Label>();
		Set<Node> nodeSet = new HashSet<Node>();
		Set<Node> targets = new HashSet<Node>();
		List<Node> nodes = new ArrayList<Node>();
				
		try (Transaction tx = g.beginTx()) {
			for(RelationshipType relType : GlobalGraphOperations.at(g).getAllRelationshipTypes()) {
				// the valid relationship that are actually in the graph
				if(edgeTypes.contains(relType.name())){
					relTypes.add(relType);
				}
			}
			
			for(Label l: GlobalGraphOperations.at(g).getAllLabels()){
				if(include.contains(l.name())){
					includeLabels.add(l);
				}
				if(target.contains(l.name())){
					targetLabels.add(l);
				}
			}
			
			for(Label includeLabel : includeLabels){
				ResourceIterator<Node> it = g.findNodes(includeLabel);
				while(it.hasNext()){
					nodeSet.add(it.next());
				}
			}
			
			nodes.addAll(nodeSet);
			
			if(nodes.size() == Integer.MAX_VALUE){
				log.severe("More nodes than int can hold. I will not be able to rand them all uniformly.");
				throw new IllegalArgumentException("More nodes than int can hold. I will not be able to rand them all uniformly.");
			}
			
			
			log.info("num nodes included: " + nodes.size());
			
			for(Label targetLabel : targetLabels){
				ResourceIterator<Node> it = g.findNodes(targetLabel);
				while(it.hasNext()){
					targets.add(it.next());
				}
			}
			
			log.info("num target nodes: " + targets.size());
			
			tx.success();
		}
		
		try (Transaction tx = g.beginTx()){
			
			Set<Node> replacementNodes = new HashSet<Node>();
			
			Random rand = new Random();
			
			if(postfix == null){
				postfix = String.valueOf(rand.nextInt());
			}

			while(replacementNodes.size() < targets.size()){
				int replacement = rand.nextInt(nodes.size());
				replacementNodes.add(nodes.get(replacement));
			}
			
			log.info("num replacement nodes: " + replacementNodes.size());
			
			Iterator<Node> targetIt = targets.iterator();
			Iterator<Node> replacementIt = replacementNodes.iterator();
			
			while(targetIt.hasNext()){
				Node target = targetIt.next();
				Node replacement = replacementIt.next();
				
				for(Relationship r : target.getRelationships(relTypes.toArray(new RelationshipType[relTypes.size()]))){
					Node other = r.getOtherNode(target);
					
					String newType = r.getType().name() + postfix;
					
					Relationship newR = replacement.createRelationshipTo(other, DynamicRelationshipType.withName(newType));
					for(String key : r.getPropertyKeys()){
						newR.setProperty(key, r.getProperty(key));
					}
				}
				
			}
			
			tx.success();
		}
	
	}

	@Override
	public void setExtraParameters(List<String> extras) {
		if(extras != null){
			log.info("processing extra parameters: " + extras.toString());

			for(String extra : extras){
				String values[] = extra.split("=",-1);

				switch(values[0]) {

				case "edgeType":
					edgeTypes.add(values[1]);
					break;

				case "target":
					target.add(values[1]);
					break;
				
				
				case "include":
					include.add(values[1]);
					break;
					
				case "postfix":
					postfix = values[1];
					break;
				}
			}

			log.info("using postfix: " + postfix);
			log.info("using include labels: " + include);
			log.info("using target label: " + target);
			log.info("using edgeTypes" + edgeTypes);
		}

	}

	@Override
	public void setDictionary(Dictionary dict) {
		this.dict = dict;

	}

}
