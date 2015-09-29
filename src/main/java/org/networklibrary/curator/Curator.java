package org.networklibrary.curator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Logger;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.schema.IndexCreator;
import org.networklibrary.core.parsing.ParsingErrorException;
import org.networklibrary.curator.config.CuratorConfigManager;
import org.networklibrary.curator.consolidating.MetaAnalysisConsolidator;
import org.networklibrary.curator.consolidating.MirTargetingConsolidator;
import org.networklibrary.curator.heattransfer.MetaHeatTransfer;


public class Curator {

	protected static final Logger log = Logger.getLogger(Curator.class.getName());

	private static Map<String,Class> parsers = new HashMap<String,Class>();
	private static Map<String,String> supported = new HashMap<String,String>();

	static {
		addParser("MIRC", "Merges miR interactions into a consolidated edge",MirTargetingConsolidator.class);
		addParser("METAC", "Consolidated the meta analysis edges",MetaAnalysisConsolidator.class);
		addParser("METAHEAT", "heat transfer for the meta analysis",MetaHeatTransfer.class);
		addParser("INDEX", "creates an index on a label and proeprty",IndexCreator.class);
	}

	public static void addParser(String cmd, String name, Class parser){
		parsers.put(cmd,parser);
		supported.put(cmd, name);
	}

	private String db;
	private CuratorConfigManager confMgr;
	private List<String> extras;

	public Curator(String db, CuratorConfigManager confMgr, List<String> extras) {
		this.db = db;
		this.confMgr = confMgr;
		this.extras = extras;
	}

	public void execute() throws ParsingErrorException {

		GraphDatabaseService g = new GraphDatabaseFactory().newEmbeddedDatabase(db);
		
		log.info("using database: " + db);
		
		try {
			Task t = makeTask();
			t.setGraph(g);
			t.setExtraParameters(extras);

			t.execute();

		} catch (ParsingErrorException e){
			log.severe("error during curating: " + e.getMessage());
			g.shutdown();
			throw(e);
		}

		g.shutdown();

	}

	protected String getType() {
		return getConfig().getType();
	}

	protected String getDb() {
		return db;
	}

	protected CuratorConfigManager getConfig() {
		return confMgr;
	}

	protected Map<String,Class> getParsers(){
		return parsers;
	}

	public static String printSupportedTypes() {
		StringBuilder buff = new StringBuilder();

		for(Entry<String,Class> p : parsers.entrySet() ){
			buff.append("\t" + p.getKey() + " = " + supported.get(p.getKey()));
			buff.append("\n");
		}

		return buff.toString();
	}

	protected Task makeTask() throws ParsingErrorException {
		Task t = null;

		try {
			log.info("Have type = " + getType() + " -> parser = " + parsers.get(getType()));		
			t = (Task)getParsers().get(getType()).newInstance();
			t.setDictionary(confMgr);
		} catch (InstantiationException e) {
			log.severe("InstantiationException when creating parser for: " + getType() + ": " + e.getMessage());
			throw new ParsingErrorException(e.getMessage());
		} catch (IllegalAccessException e) {
			log.severe("IllegalAccessException when creating parser for: " + getType() + ": " + e.getMessage());
			throw new ParsingErrorException(e.getMessage());
		}

		return t;
	}

}
