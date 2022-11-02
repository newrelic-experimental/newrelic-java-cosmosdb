package com.newrelic.instrumentation.azure.cosmos44;

import com.newrelic.agent.bridge.datastore.DatastoreVendor;
import com.newrelic.agent.bridge.datastore.JdbcDatabaseVendor;

public class CosmosDBVendor extends JdbcDatabaseVendor {
	
	public CosmosDBVendor() {
		super("CosmosDB","cosmosdb",false);
	}
 
	@Override
	public DatastoreVendor getDatastoreVendor() {
		return DatastoreVendor.JDBC;
	}

}
