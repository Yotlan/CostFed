package org.aksw.simba.quetsal.configuration;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.WatchService;
import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
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
 * Quetzal configurations setup. Need to run one time in the start before query
 * execution
 * 
 * @author Saleem
 *
 */
public class QuetzalConfig {
	static Logger log = LoggerFactory.getLogger(Config.class);
	public static RepositoryConnection con = null;

	public ArrayList<String> dataSources = new ArrayList<String>();
	public ArrayList<String> commonPredicates = new ArrayList<String>(); // list of common predicates. Note we use this
																			// in ASK_dominent Source selection
																			// Algorithm
	public double commonPredThreshold; // A threshold value for a predicate ( in % of total data sources) to be
										// considered in common predicate list

	WatchService watcher;

	// public Summary summary_;
	// public static SailRepository repo;

	public String InputFedSummaries;

	public static enum Mode {
		INDEX_DOMINANT,
		ASK_DOMINANT
	}

	public Mode mode; // Index_dominant , ASK_dominant. In first type of mode we make use of sbj, obj
						// authorities to find relevant sources for triple patterns with bound subject
						// or objects e.g ?s owl:sameAs <http://dbpedia.org/resource/Barack_Obama>, we
						// will perform index lookup for predicate owl:sameAs and objAuthority
						// <http://dbpedia.org> and all the qualifying sources will be added to the set
						// of capable sources for that triple pattern.
	// In hybrid mode we make use of SPARQL ASK queries for bound subjects or
	// objects of a common predicate such as owl:sameAs. If Predicate is not common
	// then we use index sbj ,obj authorities parts as explained above

	/**
	 * Quetzal Configurations. Must call this method once before starting source
	 * selection.
	 * mode can be either set to Index_dominant or ASK_dominant. See details in
	 * FedSum paper.
	 */
	public QuetzalConfig(Config cfg) {
		try {
			watcher = FileSystems.getDefault().newWatchService();
			mode = Mode.valueOf(cfg.getProperty("quetzal.mode", "ASK_DOMINANT"));
			commonPredThreshold = Double.parseDouble(cfg.getProperty("quetzal.inputCommonPredThreshold", "0.33"));
			//InputFedSummaries = cfg.getProperty("quetzal.fedSummaries");
		} catch (IOException e) {
			throw new FedXException(e);
		}
	}

	public void initialize(String summary) {
		loadFedSummaries(summary);
		// System.out.println("Index Load Time: "+
		// (System.currentTimeMillis()-startTime));

		loadDataSources();
		if (mode == Mode.ASK_DOMINANT) {
			loadCommonPredList();
		}
	}

	/**
	 * Initialize list of SPARQL endpoints from FedSummaires
	 * 
	 * @throws Exception Errors
	 */

	public void loadDataSources() {
		String queryString = "SELECT DISTINCT ?url WHERE {?s <http://aksw.org/quetsal/url> ?url }";
		TupleQuery tupleQuery = con.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
		TupleQueryResult result = tupleQuery.evaluate();
		while (result.hasNext()) {
			dataSources.add(result.next().getValue("url").stringValue());
		}
		// FedXFactory.initializeSparqlFederation(dataSources);
	}

	/**
	 * Load common predicate list using the threshold value specified as input
	 */
	public void loadCommonPredList() {

		String queryString = "Prefix ds:<http://aksw.org/quetsal/> "
				+ "SELECT DISTINCT ?p "
				+ " WHERE {?s ds:predicate ?p. }";

		TupleQuery tupleQuery = con.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
		TupleQueryResult result = tupleQuery.evaluate();
		ArrayList<String> fedSumPredicates = new ArrayList<String>();
		while (result.hasNext()) {
			fedSumPredicates.add(result.next().getValue("p").stringValue());
		}
		// ---check each distinct

		// int dscount = summary_.lookupSources(null, null, null).size();
		int dscount = dataSources.size();

		for (String predicate : fedSumPredicates) {
			int count = 0;
			queryString = "Prefix ds:<http://aksw.org/quetsal/> "
					+ "SELECT  Distinct ?url "
					+ " WHERE {?s ds:url ?url. "
					+ " 		?s ds:capability ?cap. "
					+ "		   ?cap ds:predicate <" + predicate + "> }";

			tupleQuery = con.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
			result = tupleQuery.evaluate();
			while (result.hasNext()) {
				result.next();
				count++;
			}
			double threshold = (double) count / dscount;
			if (threshold >= commonPredThreshold) {
				commonPredicates.add(predicate);
			}
		}
		// System.out.println(commonPredicates);
	}

	/**
	 * Load FedSummaries file into sesame in memory model
	 * 
	 * @param FedSummaries Summaries file
	 */
	public static void loadFedSummaries(String FedSummaries) {
		File curfile = new File("costfed/summaries/memorystore.data");
		curfile.delete();
		File fileDir = new File("costfed/summaries");
		Repository myRepository = new SailRepository(new MemoryStore(fileDir));
		try {
			File file = new File(FedSummaries);
			myRepository.initialize();
			System.out.println("TEST");
			con = myRepository.getConnection();
			con.add(file, "aksw.org.simba", RDFFormat.N3);
		} catch (RepositoryException e) {
			e.printStackTrace();
		} catch (RDFParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
