package org.networklibrary.curator.consolidating;

import java.util.ArrayList;
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
import org.neo4j.tooling.GlobalGraphOperations;
import org.networklibrary.core.config.Dictionary;
import org.networklibrary.curator.Task;

public class MirTargetingConsolidator implements Task {

	protected static final Logger log = Logger.getLogger(MirTargetingConsolidator.class.getName());

	protected GraphDatabaseService g = null;

	protected Set<String> edgeTypes = new HashSet<String>();
	protected Set<String> mirLabels = new HashSet<String>();
	protected String name = "cons_mir_targeting";

	private Double normfactor = null;
	private String weightName = "weight";

	private Dictionary dict;

	@Override
	public void setGraph(GraphDatabaseService g) {
		this.g = g;
	}

	@Override
	public void execute() {

		Set<RelationshipType> relTypes = new HashSet<RelationshipType>();
		Set<Node> miRs = new HashSet<Node>();

		//		try (Transaction tx = g.beginTx()){
		//			for(Node n : GlobalGraphOperations.at(g).getAllNodes()){
		//				StringBuilder b = new StringBuilder();
		//				b.append(n.getId() + " " + n.getProperty("name","unknown"));
		//				for(Label l : n.getLabels()){
		//					b.append(" " + l.name());
		//				}
		//				System.out.println(b.toString());
		//			}
		//		}

		try (Transaction tx = g.beginTx()) {
			for(RelationshipType relType : GlobalGraphOperations.at(g).getAllRelationshipTypes()) {
				// the valid relationship that are actually in the graph
				if(edgeTypes.contains(relType.name())){
					relTypes.add(relType);
				}
			}

			for(String labelName : mirLabels){

				ResourceIterator<Node> it = g.findNodes(DynamicLabel.label(labelName));
				while(it.hasNext()){
					miRs.add(it.next());
				}

			}
			tx.success();
		}

		int newRels = 0;

		for(Node miR : miRs){

			try (Transaction tx = g.beginTx()){
				Map<Node,List<Relationship>> neighbours = new HashMap<Node,List<Relationship>>();

				for(Relationship rel : miR.getRelationships(Direction.OUTGOING, relTypes.toArray(new RelationshipType[relTypes.size()])) ){
					addNeighbour(miR,neighbours,rel);
				}


				for(Entry<Node,List<Relationship>> e : neighbours.entrySet()){

					Relationship cons = miR.createRelationshipTo(e.getKey(), DynamicRelationshipType.withName(name));

					cons.setProperty("data_source", "miRTargetingConsolidator");

					
//					int numRels = e.getValue().size();

					double weight = calcWeight(e);

					cons.setProperty(getWeightName(), weight);
					cons.setProperty("dbs", getDbName(e.getValue()));
					++newRels;
				}
				tx.success();
			}
		}
		log.info("created " + newRels + " new Relationships");
	}

	private double calcWeight(Entry<Node, List<Relationship>> e) {
		String[] dbs = getDbName(e.getValue());
		
		double weight = dbs.length;
		
		if(normfactor != null && normfactor > 0.0){
			weight = weight / normfactor;
		}
		
		return weight;
	}

	private String[] getDbName(List<Relationship> rels) {

		Set<String> res = new HashSet<String>();

		for(Relationship r : rels){
			if(r.hasProperty("data_source")){
				res.add((String)r.getProperty("data_source"));
			} else {
				res.add(r.getType().name());
			}
		}

		return res.toArray(new String[res.size()]);
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
		log.info("processing extra parameters: " + extras.toString());

		for(String extra : extras){
			String values[] = extra.split("=",-1);

			switch(values[0]) {
			case "edgeType":
				edgeTypes.add(values[1]);
				break;

			case "mirlabel":
				mirLabels.add(values[1]);
				break;


			case "name":
				name = values[1];
				break;

			case "normfactor":
				normfactor = Double.valueOf(values[1]);
				break;
				
			case "weightname":
				weightName = values[1];
				break;
			}

		}

		log.info("using edgeTypes: " + edgeTypes);
		log.info("using miR Labels: " + mirLabels);
		log.info("using name: " + name);
		log.info("using normfactor: " + normfactor);
		log.info("using weightname: " + weightName);
		//		log.info("using a header: " + header);

	}

	@Override
	public void setDictionary(Dictionary dict) {
		this.dict = dict;
	}

	public String getWeightName(){
		return weightName;
	}


}
