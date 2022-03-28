package com.newrelic.instrumentation.azure.cosmos;

import com.newrelic.agent.database.DatabaseService;
import com.newrelic.agent.database.DefaultDatabaseStatementParser;
import com.newrelic.agent.database.ParsedDatabaseStatement;
import com.newrelic.agent.service.ServiceFactory;

public class CosmosUtils {

	public static final String COSMOSDB = "CosmosDB";
	private static DefaultDatabaseStatementParser parser = null;
	private static CosmosDBVendor vendor = new CosmosDBVendor();
	public static boolean initialized = false;
	
	public static void init() {
		DatabaseService dbService = ServiceFactory.getDatabaseService();
		if(dbService != null) {
			parser = (DefaultDatabaseStatementParser)dbService.getDatabaseStatementParser();
			initialized = true;
		}
	}
	
	public static ParsedDatabaseStatement parseSQL(String sql) {
		if(!initialized) {
			init();
		}
		
		if(initialized) {
			ParsedDatabaseStatement statement = parser.getParsedDatabaseStatement(vendor, sql, null);
			return statement;
		}
		
		return null;
	}
	
	public static String getIDFromLink(String link) {
		String[] parts = link.split("/");
		
		int length = parts.length;
		
		return parts[length-1];
	}
}
