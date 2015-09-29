package org.networklibrary.curator.consolidating;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.logging.Logger;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.networklibrary.core.config.Dictionary;
import org.networklibrary.curator.Task;

public class MetaAnalysisConsolidator implements Task {

	protected static final Logger log = Logger.getLogger(MetaAnalysisConsolidator.class.getName());

	protected GraphDatabaseService g = null;

	protected String outcomeLabel = "outcome";

	private Dictionary dict;

	public enum Types {
		neutral,pro,anti
	}

	@Override
	public void setGraph(GraphDatabaseService g) {
		this.g = g;
	}

	@Override
	public void execute() {

		Set<RelationshipType> edgeTypes = new HashSet<RelationshipType>();
		Set<Node> outcomes = new HashSet<Node>();

		try (Transaction tx = g.beginTx()) {
			
			edgeTypes.add(DynamicRelationshipType.withName(Types.neutral.name()));
			edgeTypes.add(DynamicRelationshipType.withName(Types.anti.name()));
			edgeTypes.add(DynamicRelationshipType.withName(Types.pro.name()));
			
			
			ResourceIterator<Node> it = g.findNodes(DynamicLabel.label(outcomeLabel));
			while(it.hasNext()){
				outcomes.add(it.next());
			}
			tx.success();
		}

		int newRels = 0;
		try (Transaction tx = g.beginTx()){
			for(Node outcome : outcomes){
				Map<Node,List<Relationship>> neighbours = new HashMap<Node,List<Relationship>>();

				for(Relationship rel : outcome.getRelationships(Direction.INCOMING,edgeTypes.toArray(new RelationshipType[edgeTypes.size()])) ){
					addNeighbour(outcome,neighbours,rel);
				}

				for(Entry<Node,List<Relationship>> e : neighbours.entrySet()){

					EnumMap<Types, Integer> counts = countTypes(e.getValue());

					Types type = decideType(counts);

					String relName = makeName(type);

					Relationship cons = e.getKey().createRelationshipTo(outcome, DynamicRelationshipType.withName(relName));

					cons.setProperty("data_source", "MetaAnalysisConsolidator");

					double weight = counts.get(type) / (double)e.getValue().size();

					cons.setProperty("weight", weight);
					addCounts(cons,counts);

					++newRels;
				}
			}

			tx.success();
		}
		log.info("created " + newRels + " new Relationships");
	}

	private void addCounts(Relationship r, EnumMap<Types, Integer> counts) {
		for(Entry<Types,Integer> e : counts.entrySet()){
			r.setProperty(e.getKey().name(), e.getValue());
		}
	}

	private String makeName(Types type) {
		String name = "c" + type.name().toLowerCase();
		return name;
	}

	// well still not taking good care of cases where anti == pro but for now not crucial
	private Types decideType(EnumMap<Types, Integer> counts) {
		int anti = counts.get(Types.anti);
		int pro = counts.get(Types.pro);
		int neutral = counts.get(Types.neutral);

//		int max = -1;
//
//		for(Entry<Types,Integer> e : counts.entrySet()){
//			if(e.getValue() > max){
//				max = e.getValue();
//				type = e.getKey();
//			}
//		}
		
		if(anti == 0 && pro == 0 && neutral > 0){
			return Types.neutral;
		}
		
		return anti > pro ? Types.anti : Types.pro;
	}

	private EnumMap<Types, Integer> countTypes(List<Relationship> rels) {
		EnumMap<Types, Integer> counts = new EnumMap<Types,Integer>(Types.class);
		
		for(Types t : Types.values()){
			counts.put(t, 0);
		}

		for(Relationship r : rels){
			addType(counts, Types.valueOf(r.getType().name()));
		}

		return counts;
	}

	private void addType(EnumMap<Types, Integer> counts, Types type) {
		if(!counts.containsKey(type)){
			counts.put(type, 0);
		}
		counts.put(type, counts.get(type)+1);
	}

	private void addNeighbour(Node you,Map<Node, List<Relationship>> neighbours, Relationship rel) {

		Node other = rel.getOtherNode(you);
		if(!neighbours.containsKey(other)){
			neighbours.put(other, new ArrayList<Relationship>());
		}
		neighbours.get(other).add(rel);
	}

	@Override
	public void setExtraParameters(List<String> extras) {
		if(extras != null){
			log.info("processing extra parameters: " + extras.toString());

			for(String extra : extras){
				String values[] = extra.split("=",-1);

				switch(values[0]) {
				case "outcome_label":
					outcomeLabel = values[1];
					break;
				}

			}

			log.info("using outcome label: " + outcomeLabel);
		}

	}

	@Override
	public void setDictionary(Dictionary dict) {
		this.dict = dict;
	}



}
