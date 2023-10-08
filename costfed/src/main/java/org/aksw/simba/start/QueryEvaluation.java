package org.aksw.simba.start;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryInterruptedException;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.explanation.Explanation;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailTupleQuery;
import com.fluidops.fedx.Config;
import com.fluidops.fedx.FedXConnection;
import com.fluidops.fedx.FedXFactory;
import com.fluidops.fedx.QueryManager;
import com.fluidops.fedx.structures.QueryInfo;
import com.fluidops.fedx.algebra.StatementSource;
import com.fluidops.fedx.sail.FedXSailRepositoryConnection;

import org.aksw.simba.quetsal.configuration.QuetzalConfig;
import org.aksw.sparql.query.algebra.helpers.BGPGroupGenerator;

public class QueryEvaluation {
	protected static final Logger log = LoggerFactory.getLogger(QueryEvaluation.class);
	static {
		try {
			ClassLoader.getSystemClassLoader().loadClass("org.slf4j.LoggerFactory"). getMethod("getLogger", ClassLoader.getSystemClassLoader().loadClass("java.lang.String")).
			 invoke(null,"ROOT");
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}
	
	QueryProvider qp;

	public QueryEvaluation(String queries) throws Exception {
		qp = new QueryProvider(queries);
	}
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception 
	{
		String cfgName = args[0];
		String resultfile = args[2];
		String provenancefile = args[3];
		String explanationfile = args[4];
		// String statfile = args[5];
		String timeout = args[5];
		String summary = args[6];
		String queries = args[7];
		String noExec = args[8];

		String localhost = args[1];
		List<String> endpoints = new ArrayList<>();

		for(int i=9;i<args.length;++i){
			endpoints.add(localhost+"/?default-graph-uri="+args[i]);
		}
		
		//String host = "localhost";
		//String host = "ws24348.avicomp.com";
		//String host = "192.168.0.145";
		//String queries = "S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 C1 C2 C3 C4 C6 C7 C8 C9 C10"; //"C1 C3 C5 C6 C7 C8 C9 C10 L1 L2 L3 L4 L5 L6 L7 L8";
		//String queries = "S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 C1 C2 C3 C6 C7 C8 C9 C10";
		//String queries = "S1 S2 S3 S4 S5 S6 S7 S8 S9 S10 S11 S12 S13 S14 C1 C2 C3 C4 C6 C7 C8 C9 C10";
		//String queries = "CH3"; // S3 C6 C2
		/*
		List<String> endpointsMin = Arrays.asList(
			 "http://" + host + ":8890/sparql",
			 "http://" + host + ":8891/sparql",
			 "http://" + host + ":8892/sparql",
			 "http://" + host + ":8893/sparql",
			 "http://" + host + ":8894/sparql",
			 "http://" + host + ":8895/sparql",
			 "http://" + host + ":8896/sparql",
			 "http://" + host + ":8897/sparql",
			 "http://" + host + ":8898/sparql"
		);
		
		List<String> endpointsMax = Arrays.asList(
			 "http://" + host + ":8890/sparql",
			 "http://" + host + ":8891/sparql",
			 "http://" + host + ":8892/sparql",
			 "http://" + host + ":8893/sparql",
			 "http://" + host + ":8894/sparql",
			 "http://" + host + ":8895/sparql",
			 "http://" + host + ":8896/sparql",
			 "http://" + host + ":8897/sparql",
			 "http://" + host + ":8898/sparql"
			 
			 , "http://" + host + ":8887/sparql"
			 , "http://" + host + ":8888/sparql"
			 , "http://" + host + ":8889/sparql"
			 , "http://" + host + ":8899/sparql"
		);
			
		List<String> endpointsTest = Arrays.asList(
				   "http://" + host + ":8887/sparql"
				 , "http://" + host + ":8888/sparql"
				 , "http://" + host + ":8889/sparql"
				 , "http://" + host + ":8899/sparql"	
		);
	
		List<String> endpointsMin2 = Arrays.asList(
			 "http://" + host + ":8890/sparql",
			 "http://" + host + ":8891/sparql",
			 "http://" + host + ":8892/sparql",
			 "http://" + host + ":8893/sparql",
			 "http://" + host + ":8894/sparql",
			 "http://" + host + ":8895/sparql",
			 "http://" + host + ":8896/sparql",
			 "http://" + host + ":8897/sparql",
			 "http://" + host + ":8898/sparql",
			 "http://" + host + ":8899/sparql"
		);

		List<String> endpointsSake = Arrays.asList(
		        "http://144.76.166.111:8900/sparql",
		        "http://144.76.166.111:8901/sparql"
		);
		
		List<String> endpoints = endpointsMin2;
		*/
		Map<String, List<List<Object>>> reports = multyEvaluate(queries, 1, cfgName, endpoints, Integer.valueOf(timeout), explanationfile, resultfile, provenancefile, noExec);
	
		/*for (Map.Entry<String, List<List<Object>>> e : reports.entrySet())
		{
			List<List<Object>> report = e.getValue();
			String r = printReport(report);
			log.info(r);
			if (e.getKey() == "queryexplain") {
				FileUtils.write(new File(explanationfile), r);
			}
			if (e.getKey() == "report") {
				FileUtils.write(new File(resultfile), r);
			}
			if (e.getKey() == "sstreport") {
				FileUtils.write(new File(provenancefile), r);
			}
			if (e.getKey() == "stat") {
				FileUtils.write(new File(statfile), r);
			}
		}*/

		System.exit(0);
	}
	
	public Map<String, List<List<Object>>> evaluate(String queries, String cfgName, List<String> endpoints, int timeout, String explanationfile, String resultfile, String provenancefile, String noExec) throws Exception {
		List<List<Object>> report = new ArrayList<List<Object>>();
		List<List<Object>> sstreport = new ArrayList<List<Object>>();
		List<List<Object>> queryexplain = new ArrayList<List<Object>>();
		List<List<Object>> stat = new ArrayList<List<Object>>();
		Map<String, List<List<Object>>> result = new HashMap<String, List<List<Object>>>();
		result.put("report", report);
		result.put("sstreport", sstreport);
		result.put("queryexplain", queryexplain);
		result.put("stat", stat);

		String homeDir = Paths.get(provenancefile).getParent().toString();
		String sourceSelectionTimeFile = homeDir + "/source_selection_time.txt";
		String planningTimeFile = homeDir + "/planning_time.txt";
		String askFile = homeDir + "/ask.txt";
		String execTimeFile = homeDir + "/exec_time.txt";
		
		List<String> qnames = Arrays.asList(queries.split(" "));
		//System.out.println("QUERIES NAMES"+qnames.toString());
		for (String curQueryName : qnames)
		{
			List<Object> reportRow = new ArrayList<Object>();
			report.add(reportRow);
			String curQuery = qp.getQuery(curQueryName);
			reportRow.add(curQueryName);
			
			List<Object> sstReportRow = new ArrayList<Object>();
			sstreport.add(sstReportRow);
			sstReportRow.add(curQueryName);

			List<Object> queryExplainRow = new ArrayList<Object>();
			queryexplain.add(queryExplainRow);
			queryExplainRow.add(curQueryName);

			List<Object> statRow = new ArrayList<Object>();
			stat.add(statRow);
			statRow.add(curQueryName);
			
			Config config = new Config(cfgName);
			SailRepository repo = null;
			TupleQueryResult res = null;

			List<StatementPattern> stmtPattern = BGPGroupGenerator.generateBgpGroups(curQuery).get(0);
			Map<StatementPattern, List<StatementSource>> rows = new LinkedHashMap<>();
			for (StatementPattern stmt: stmtPattern) {
				rows.put(stmt, new ArrayList<>());
			}
			
			try {
				repo = FedXFactory.initializeSparqlFederation(config, endpoints);
				TupleQuery query = repo.getConnection().prepareTupleQuery(QueryLanguage.SPARQL, curQuery);
				query.setMaxExecutionTime(timeout);
				//System.out.println("TupleQuery: "+query);

				FedXConnection fconn = (FedXConnection) repo.getConnection().getSailConnection();
				QueryManager qManager = fconn.getQueryManager();
				String qPlan = qManager.getQueryPlan(curQuery,fconn.getSummary());

				queryExplainRow.add(qPlan);
				String r2 = printReport(queryexplain);
				FileUtils.write(new File(explanationfile), r2);
				
			   	long startTime = System.currentTimeMillis();
			   	res = query.evaluate();
			    long count = 0;

				FileUtils.write(new File(sourceSelectionTimeFile), String.valueOf(QueryInfo.queryInfo.get().getSourceSelection().time));
				FileUtils.write(new File(planningTimeFile), String.valueOf(QueryInfo.queryInfo.get().getSourceSelection().planningTime));
				FileUtils.write(new File(askFile), String.valueOf(QueryInfo.queryInfo.get().getSourceSelection().nbAskQuery));

				Map<StatementPattern, List<StatementSource>> stmtToSources = QueryInfo.queryInfo.get().getSourceSelection().getStmtToSources();
				for (StatementPattern stmt : stmtToSources.keySet()) {
					rows.replace(stmt, stmtToSources.get(stmt));
				}
				for (StatementPattern temp_row : rows.keySet()) {
					Map<String, String> row = new LinkedHashMap<>();
					row.put(temp_row.toString().trim().replace("\n", "").replace(",", ";"), rows.get(temp_row).toString().trim().replace("\n", "").replace(",", ";"));
					sstReportRow.add(row);
				}
				String r1 = printReport(sstreport);
				FileUtils.write(new File(provenancefile), r1);
			
				//log.info("RESULT\n");
				if(!Boolean.valueOf(noExec)){
					while (res.hasNext()) {
						BindingSet row = res.next();
						//System.out.println(count+": "+ row);
						reportRow.add((BindingSet)row);
						count++;
					}
					while (res.hasNext()) {
						BindingSet row = res.next();
						//System.out.println(count+": "+ row);
						reportRow.add((BindingSet)row);
						count++;
					}

					String r3 = printReport(report);
					FileUtils.write(new File(resultfile), r3);
				
					long runTime = System.currentTimeMillis() - startTime;
					log.info("QUERY\n"+curQuery);
					log.info(curQueryName + ": Query exection time (msec): "+ runTime + ", Total Number of Records: " + count + ", Source count: " + QueryInfo.queryInfo.get().numSources.longValue());
					//log.info(curQueryName + ": Query exection time (msec): "+ runTime + ", Total Number of Records: " + count + ", Source Selection Time: " + QueryInfo.queryInfo.get().getSourceSelection().time);
					FileUtils.write(new File(execTimeFile), String.valueOf(runTime));
				}
			} catch (QueryInterruptedException e) {
			} catch (Throwable e) {
				e.printStackTrace();
				log.error("", e);
				File f = new File("results/" + curQueryName + ".error.txt");
				ByteArrayOutputStream os = new ByteArrayOutputStream();
				PrintStream ps = new PrintStream(os);
				e.printStackTrace(ps);
				ps.flush();
				FileUtils.write(f, os.toString("UTF8"));
				reportRow.add(null); reportRow.add(null);
			} finally {
				if (null != res) {
					//System.out.println("CLOSE...");
		    		res.close();
					//System.out.println("CLOSE!");
		    	}
				
		    	if (null != repo) {
					//System.out.println("SHUTDOWN...");
		    	    repo.shutDown();
					//System.out.println("SHUTDOWN!");
		    	}
	        }
		}
		//System.out.println("RESULT");
		return result;
	}
	
	static Map<String, List<List<Object>>> multyEvaluate(String queries, int num, String cfgName, List<String> endpoints, int timeout, String explanationfile, String resultfile, String provenancefile, String noExec) throws Exception {
		String queriesPath = queries.split("injected.sparql")[0];
		System.out.println("QUERIES PATH: "+queries);
		String queriesName = queries.split("/")[queries.split("/").length-1];
		System.out.println("QUERIES NAME: "+queries);
		QueryEvaluation qeval = new QueryEvaluation(queriesPath);

		Map<String, List<List<Object>>> result = null;
		for (int i = 0; i < num; ++i) {
			Map<String, List<List<Object>>> subReports = qeval.evaluate(queriesName, cfgName, endpoints, timeout, explanationfile, resultfile, provenancefile, noExec);
			//System.out.println("SUBREPORTS");
			//System.out.println(subReports);
			if (i == 0) {
				result = subReports;
			} else {
				//assert(report.size() == subReport.size());
				for (Map.Entry<String, List<List<Object>>> e : subReports.entrySet())
				{
					List<List<Object>> subReport = e.getValue();
					for (int j = 0; j < subReport.size(); ++j) {
						List<Object> subRow = subReport.get(j);
						List<Object> row = result.get(e.getKey()).get(j);
						row.add(subRow.get(2));
					}
				}
			}
		}
		
		//System.out.println("RESULT");
		//System.out.println(result);
		return result;
	}
	
	static String printReport(List<List<Object>> report) {
		if (report.isEmpty()) return "";
		
		StringBuilder sb = new StringBuilder();
		sb.append("Result #").append(0);
		
		List<Object> firstRow = report.get(0);
		for (int i = 2; i < firstRow.size(); ++i) {
			sb.append(",Result #").append(i-1);
		}
		sb.append("\n");
		for (List<Object> row : report) {
			//sb.append((repfile.split("/")[repfile.split("/").length-1]).split("-")[0]);
			//sb.append(",");
			for (int c = 1; c < row.size(); ++c) {
				sb.append(row.get(c));
				if (c != row.size() - 1) {
					sb.append(",");
				}
			}
			sb.append("\n");
		}
		return sb.toString();
	}
}