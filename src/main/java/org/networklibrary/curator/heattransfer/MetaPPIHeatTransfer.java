package org.networklibrary.curator.heattransfer;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.tooling.GlobalGraphOperations;
import org.networklibrary.core.config.Dictionary;
import org.networklibrary.curator.Task;

public class MetaPPIHeatTransfer implements Task {

	protected static final Logger log = Logger.getLogger(MetaHeatTransfer.class.getName());

	protected GraphDatabaseService g = null;

	protected Set<String> edgeTypes = new HashSet<String>();


	protected String outcomeLabel = "outcome";

	protected String postfix = "_effect";
	
	protected String firstProperty = "weight";
	
	protected String secondProperty = "combined_score";
	
	protected int cutoff = 700;
	
	private Dictionary dict;
	
	@Override
	public void setGraph(GraphDatabaseService g) {
		this.g = g;
	}

	@Override
	public void execute() {
		Set<RelationshipType> relTypes = new HashSet<RelationshipType>();
		Set<Node> outcomes = new HashSet<Node>();
	
		try (Transaction tx = g.beginTx()) {
			for(RelationshipType relType : GlobalGraphOperations.at(g).getAllRelationshipTypes()) {
				// the valid relationship that are actually in the graph
				if(edgeTypes.contains(relType.name())){
					relTypes.add(relType);
				}
			}

			ResourceIterator<Node> it = g.findNodes(DynamicLabel.label(outcomeLabel));
			while(it.hasNext()){
				outcomes.add(it.next());
			}
			tx.success();
		}

		for(Node outcome : outcomes) {
			try (Transaction tx = g.beginTx()) {
				
				// we operate under the assumption that only one edge connects two nodes. the consolidating should have ensured that
				for(Relationship rel : outcome.getRelationships(Direction.INCOMING,relTypes.toArray(new RelationshipType[relTypes.size()])) ){

					Node neighbour = rel.getOtherNode(outcome);

					double effect = (double)rel.getProperty(firstProperty,0.0);
					String effectName = makeEffectName(outcome);

					addEffect(neighbour,effectName,effect);

					// these are the miRs with consolidated edges
					for(Relationship r : neighbour.getRelationships(Direction.OUTGOING, relTypes.toArray(new RelationshipType[relTypes.size()]))){
						Integer weight = (Integer)r.getProperty(secondProperty, 0);
						
						if(weight > cutoff){
							double mirEffect = effect * weight.doubleValue();

							Node protein = r.getOtherNode(neighbour);

							addEffect(protein,effectName,mirEffect);
						}
					}
				}
				tx.success();
			}

		}

	}

	private void addEffect(Node miR, String effectName, double mirEffect) {
		if(!miR.hasProperty(effectName)){
			miR.setProperty(effectName, 0.0);
		}

		double oldValue = (double)miR.getProperty(effectName);
		miR.setProperty(effectName, oldValue + mirEffect);
	}

	private String makeEffectName(Node node) {
		String name = ((String)node.getProperty("name")) + postfix;
		return name;
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

				case "outcome_label":
					outcomeLabel = values[1];
					break;

				case "postfix":
					postfix = values[1];
					break;
										
				case "firstprop":
					firstProperty = values[1];
					break;
					
				case "secondprop":
					secondProperty = values[1];
					break;
					
				case "cutoff":
					cutoff = Integer.valueOf(values[1]);
					break;
				}

			}

			log.info("using postfix: " + postfix);
			log.info("using outcome label: " + outcomeLabel);
			log.info("using edgeTypes" + edgeTypes);
		}

	}

	@Override
	public void setDictionary(Dictionary dict) {
		this.dict = dict;

	}

}
