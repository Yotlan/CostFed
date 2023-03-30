package org.aksw.simba.quetsal.configuration;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.WatchService;
import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.sail.memory.MemoryStore;

import com.fluidops.fedx.Config;
import com.fluidops.fedx.exception.FedXException;

/**
 * Quetzal configurations setup. Need to run one time in the start before query execution
 * @author Saleem
 *
 */
public class QuetzalConfig {
	static Logger log = LoggerFactory.getLogger(Config.class);

	// public ArrayList<String> dataSources = new ArrayList<String>();
	public ArrayList<String> commonPredicates = new ArrayList<String>(); // list of common predicates. Note we use this in ASK_dominent Source selection Algorithm
	public double commonPredThreshold;  //A threshold value for a predicate ( in % of total data sources) to be considered in common predicate list
	
	WatchService watcher;
	
	// public Summary summary_;
	// public static SailRepository repo;
	
	public static enum Mode {
		INDEX_DOMINANT,
		ASK_DOMINANT
	}
	
	public Mode mode; // Index_dominant , ASK_dominant. In first type of mode we  make use of sbj, obj authorities to find relevant sources for triple patterns with bound subject or objects e.g ?s  owl:sameAs  <http://dbpedia.org/resource/Barack_Obama>, we will perform index lookup for predicate owl:sameAs and objAuthority  <http://dbpedia.org> and all the qualifying sources will be added to the set of capable sources for that triple pattern.  
	// In hybrid mode we make use of SPARQL ASK queries for bound subjects or objects of a common predicate such as owl:sameAs. If Predicate is not common then we use index sbj ,obj authorities parts as explained above

	/**
	 * Quetzal Configurations. Must call this method once before starting source selection.
	 * mode can be either set to Index_dominant or ASK_dominant. See details in FedSum paper.
	 */
	public QuetzalConfig(Config cfg)
	{
		try {
		    watcher = FileSystems.getDefault().newWatchService();
			mode = Mode.valueOf(cfg.getProperty("quetzal.mode", "ASK_DOMINANT"));
			commonPredThreshold = Double.parseDouble(cfg.getProperty("quetzal.inputCommonPredThreshold", "0.33"));
			this.loadCommonPredList(cfg);
		} catch (IOException e) {
			throw new FedXException(e);
		}
	}
	
	// /**
	//  * Initialize list of SPARQL endpoints from FedSummaires 
	//  * @throws Exception Errors
	//  */
	// public void loadDataSources()
	// {
	// 	String queryString = "SELECT DISTINCT ?url WHERE {?s <http://aksw.org/quetsal/url> ?url }";
	// 	TupleQuery tupleQuery = con.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
	// 	TupleQueryResult result = tupleQuery.evaluate();
	// 	while(result.hasNext())
	// 	{
	// 		dataSources.add(result.next().getValue("url").stringValue());
	// 	}
	// 	FedXFactory.initializeSparqlFederation(dataSources);
	// }

	// /**
	//  * Load common predicate list using the threshold value specified as input											
	//  */
	// public void loadCommonPredList(List<RepositoryConnection> conns) {
	    
	// 	String queryString = "Prefix ds:<http://aksw.org/quetsal/> "
	// 			+ "SELECT DISTINCT ?p "
	// 			+ " WHERE {?s ds:predicate ?p. }";

	// 	for (RepositoryConnection con : conns)
	// 	{
    // 		TupleQuery tupleQuery = con.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
    // 		TupleQueryResult result = tupleQuery.evaluate();
    // 		ArrayList<String> fedSumPredicates = new  ArrayList<String>();
    // 		while(result.hasNext())
    // 		{
    // 			fedSumPredicates.add(result.next().getValue("p").stringValue());
    // 		}
    // 		//---check each distinct 
    
    // 		int dscount = summary_.lookupSources(null, null, null).size();
    		
    // 		for (String predicate : fedSumPredicates)
    // 		{
    // 			int count = 0;
    // 			queryString = "Prefix ds:<http://aksw.org/quetsal/> "
    // 					+ "SELECT  Distinct ?url "
    // 					+ " WHERE {?s ds:url ?url. "
    // 					+ " 		?s ds:capability ?cap. "
    // 					+ "		   ?cap ds:predicate <" + predicate + "> }" ;
    
    // 			tupleQuery = con.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
    // 			result = tupleQuery.evaluate();
    // 			while(result.hasNext())
    // 			{
    // 				result.next();
    // 				count++;
    // 			}
    // 			double threshold = (double) count/dscount;
    // 			if (threshold >= commonPredThreshold) {
    // 				commonPredicates.add(predicate); 
    // 			}
    // 		}
	// 	}
	// 	System.out.println(commonPredicates);
	// }

	public void loadCommonPredList(Config config) {
		File curfile = new File ("summaries/memorystore.data");
		curfile.delete();

		File fileDir = new File("summaries");
		Repository repository = new SailRepository(new MemoryStore(fileDir));

		try {
			repository.initialize();
			RepositoryConnection connection = repository.getConnection();
			try {
				String summary = config.getProperty("quetzal.fedSummaries");
				connection.add(new File(summary), "aksw.org.simba", RDFFormat.N3);
				
				// retrieving data sources
				String query = "SELECT DISTINCT ?url WHERE {?s <http://aksw.org/quetsal/url> ?url}";
				TupleQueryResult result = connection.prepareTupleQuery(QueryLanguage.SPARQL, query).evaluate();
				ArrayList<String> dataSources = new ArrayList<>();
				while(result.hasNext()) {
					dataSources.add(result.next().getValue("url").stringValue());
				}
				result.close();
				// System.out.println(dataSources);

				// retrieving common predicates
				query = "SELECT DISTINCT ?p WHERE {?s <http://aksw.org/quetsal/predicate> ?p}";
				result = connection.prepareTupleQuery(QueryLanguage.SPARQL, query).evaluate();
				ArrayList<String> predicates = new ArrayList<>();
				while(result.hasNext()) {
					predicates.add(result.next().getValue("p").stringValue());
				}
				result.close();
				// System.out.println(predicates);
				for(String predicate: predicates) {
					int count = 0;
					query = "PREFIX ds: <http://aksw.org/quetsal/> "
						+ "SELECT DISTINCT ?url "
						+ "WHERE { ?s ds:url ?url . "
						+ " 	   ?s ds:capability ?cap . "
						+ "		   ?cap ds:predicate <" + predicate + ">}";
						
					result = connection.prepareTupleQuery(QueryLanguage.SPARQL, query).evaluate();
					while(result.hasNext()) {
						result.next();
						count++;
					}
					result.close();
					double threshold = (double) count / dataSources.size();
					System.out.println(String.format("<%s>: %f = %d / %d >= %f", predicate, threshold, count, dataSources.size(), commonPredThreshold));
					if(threshold >= commonPredThreshold) {
						commonPredicates.add(predicate); 
					}		
				}
				// System.out.println(commonPredicates);

			} catch (RDFParseException|RepositoryException|IOException e) {
				e.printStackTrace();
			} finally {
				connection.close();
			}
		} catch (RepositoryException e) {
			e.printStackTrace();
		} finally {
			repository.shutDown();
		}
	}
}
