package org.networklibrary.curator;

import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.networklibrary.core.parsing.ParsingErrorException;



/**
 * Hello world!
 *
 */
public class App 
{
	protected static final Logger log = Logger.getLogger(App.class.getName());
	
    public static void main( String[] args )
    {
    	Options options = new Options();
		Option help = OptionBuilder.withDescription("Help message").create("help");
		Option dbop = OptionBuilder.withArgName("[URL]").hasArg().withDescription("Neo4j instance to prime").withLongOpt("target").withType(String.class).create("db");
		Option typeop = OptionBuilder.withArgName("[TYPE]").hasArg().withDescription("Types available:").withType(String.class).create("t");
		Option configOp = OptionBuilder.hasArg().withDescription("Alternative config file").withLongOpt("config").withType(String.class).create("c");
		Option extraOps = OptionBuilder.hasArg().withDescription("Extra configuration parameters for the import").withType(String.class).create("x");
		
		Option dictionaryOp = OptionBuilder.hasArg().withDescription("Dictionary file to use").withLongOpt("dictionary").withType(String.class).create("d");
		

		options.addOption(help);
		options.addOption(dbop);
		options.addOption(typeop);
		options.addOption(configOp);
		options.addOption(extraOps);
		options.addOption(dictionaryOp);

		CommandLineParser parser = new GnuParser();
		try {

			CommandLine line = parser.parse( options, args );

			
			if(line.hasOption("help") || args.length == 0){
				HelpFormatter formatter = new HelpFormatter();
				formatter.printHelp( "netlib-edger [OPTIONS] [FILE]", options );
				
				return;
			}

			String db = null;
			if(line.hasOption("db")){
				db = line.getOptionValue("db");
			}

			String type = null;
			if(line.hasOption("t")){
				type = line.getOptionValue("t");
			}

			String config = null;
			if(line.hasOption("c")){
				config = line.getOptionValue("c");
			}

			List<String> extras = null;
			if(line.hasOption("x")){
				extras = Arrays.asList(line.getOptionValues("x"));
			}
			
			String dictionary = null;
			if(line.hasOption("d")){
				dictionary = line.getOptionValue("d");
			}


			List<String> inputFiles = line.getArgList();

			if(config != null && !config.isEmpty()){
				log.info("user-supplied config file used: " + config);
			}


			//
			
		}
		catch( ParseException exp ) {
			// oops, something went wrong
			exp.printStackTrace();
			System.err.println( "Parsing failed.  Reason: " + exp.getMessage() );
			System.exit(-1);
		}
    }
}