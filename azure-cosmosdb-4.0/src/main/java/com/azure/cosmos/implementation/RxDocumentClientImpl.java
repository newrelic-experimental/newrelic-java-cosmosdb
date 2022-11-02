package com.azure.cosmos.implementation;

import java.util.HashMap;
import java.util.List;

import org.reactivestreams.Subscription;

import com.azure.cosmos.implementation.apachecommons.lang.tuple.Pair;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.FeedResponse;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.SqlQuerySpec;
import com.newrelic.agent.database.ParsedDatabaseStatement;
import com.newrelic.api.agent.DatastoreParameters;
import com.newrelic.api.agent.NewRelic;
import com.newrelic.api.agent.weaver.Weave;
import com.newrelic.api.agent.weaver.Weaver;
import com.newrelic.instrumentation.azure.cosmos40.CompletionRunnable;
import com.newrelic.instrumentation.azure.cosmos40.CosmosUtils;
import com.newrelic.instrumentation.azure.cosmos40.ErrorConsumer;
import com.newrelic.instrumentation.azure.cosmos40.Utils;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Weave
public abstract class RxDocumentClientImpl {

	public Mono<ResourceResponse<DocumentCollection>> createCollection(String databaseLink, DocumentCollection collection, RequestOptions options) {
		HashMap<String, Object> attributes = new HashMap<String, Object>();
		Utils.addAttribute(attributes, "DatabaseLink", databaseLink);
		String collectionName = collection.getId();
		
		Mono<ResourceResponse<DocumentCollection>> result = Weaver.callOriginal();
		DatastoreParameters params = DatastoreParameters.product(CosmosUtils.COSMOSDB).collection(collectionName).operation("createCollection").build();
		CompletionRunnable<Subscription> runnable = new CompletionRunnable<Subscription>("RxDocumentClientImpl/createCollection", params, NewRelic.getAgent().getTransaction());
		runnable.setAttributes(attributes);
		ErrorConsumer errorConsumer = new ErrorConsumer(runnable);
		return result.doOnSubscribe(runnable).doOnTerminate(runnable).doOnCancel(runnable).doOnError(errorConsumer);
	}
	
	public Mono<ResourceResponse<Database>> createDatabase(Database database, RequestOptions options) {
		String databaseName = database.getId();
		
		Mono<ResourceResponse<Database>> result = Weaver.callOriginal();
		DatastoreParameters params = DatastoreParameters.product(CosmosUtils.COSMOSDB).collection(databaseName).operation("createDatabase").build();
		CompletionRunnable<Subscription> runnable = new CompletionRunnable<Subscription>("RxDocumentClientImpl/createDatabase", params, NewRelic.getAgent().getTransaction());
		ErrorConsumer errorConsumer = new ErrorConsumer(runnable);
		return result.doOnSubscribe(runnable).doOnTerminate(runnable).doOnCancel(runnable).doOnError(errorConsumer);
	}
	
	public Mono<ResourceResponse<DocumentCollection>> deleteCollection(String collectionLink, RequestOptions options) {
		HashMap<String, Object> attributes = new HashMap<String, Object>();
		Utils.addAttribute(attributes, "ConnectionLink", collectionLink);
		String collectionName = CosmosUtils.getIDFromLink(collectionLink);
		
		Mono<ResourceResponse<DocumentCollection>> result = Weaver.callOriginal();
		DatastoreParameters params = DatastoreParameters.product(CosmosUtils.COSMOSDB).collection(collectionName).operation("deleteCollection").build();
		CompletionRunnable<Subscription> runnable = new CompletionRunnable<Subscription>("RxDocumentClientImpl/deleteCollection", params, NewRelic.getAgent().getTransaction());
		runnable.setAttributes(attributes);
		ErrorConsumer errorConsumer = new ErrorConsumer(runnable);
		return result.doOnSubscribe(runnable).doOnTerminate(runnable).doOnCancel(runnable).doOnError(errorConsumer);
	}
	
	public Mono<ResourceResponse<Database>> deleteDatabase(String databaseLink, RequestOptions options) {
		String databaseName = CosmosUtils.getIDFromLink(databaseLink);
		
		Mono<ResourceResponse<Database>> result = Weaver.callOriginal();
		DatastoreParameters params = DatastoreParameters.product(CosmosUtils.COSMOSDB).collection(databaseName).operation("deleteDatabase").build();
		CompletionRunnable<Subscription> runnable = new CompletionRunnable<Subscription>("RxDocumentClientImpl/deleteDatabase", params, NewRelic.getAgent().getTransaction());
		ErrorConsumer errorConsumer = new ErrorConsumer(runnable);
		return result.doOnSubscribe(runnable).doOnTerminate(runnable).doOnCancel(runnable).doOnError(errorConsumer);
	}
	
	public Mono<StoredProcedureResponse> executeStoredProcedure(String storedProcedureLink, RequestOptions options, List<Object> procedureParams) {
		String storedProcedure = CosmosUtils.getIDFromLink(storedProcedureLink);
		Mono<StoredProcedureResponse> result = Weaver.callOriginal();
		
		DatastoreParameters params = DatastoreParameters.product(CosmosUtils.COSMOSDB).collection(storedProcedure).operation("executeStoredProcedure").build();
		CompletionRunnable<Subscription> runnable = new CompletionRunnable<Subscription>("RxDocumentClientImpl/executeStoredProcedure", params, NewRelic.getAgent().getTransaction());
		ErrorConsumer errorConsumer = new ErrorConsumer(runnable);
		return result.doOnSubscribe(runnable).doOnTerminate(runnable).doOnCancel(runnable).doOnError(errorConsumer);
	}
	
	@SuppressWarnings("unused")
	private <T extends Resource> Flux<FeedResponse<T>> createQuery(String parentResourceLink, SqlQuerySpec sqlQuery, CosmosQueryRequestOptions options, Class<T> klass, ResourceType resourceTypeEnum) {
		Flux<FeedResponse<T>> result = Weaver.callOriginal();
		
		ParsedDatabaseStatement statement = CosmosUtils.parseSQL(sqlQuery.getQueryText());
		DatastoreParameters params = null;
		if(statement != null) {
			params = DatastoreParameters.product(CosmosUtils.COSMOSDB).collection(statement.getModel()).operation(statement.getOperation()).noInstance().noDatabaseName().slowQuery(sqlQuery.getQueryText(), Utils.getConverter()).build();
		}

		CompletionRunnable<Subscription> runnable = new CompletionRunnable<Subscription>("CosmosAsyncClient/query", params, NewRelic.getAgent().getTransaction());
		ErrorConsumer errorConsumer = new ErrorConsumer(runnable);

		return result.doOnSubscribe(runnable).doOnCancel(runnable).doOnComplete(runnable).doOnError(errorConsumer);
	}

	public <T> Mono<FeedResponse<T>> readMany(List<Pair<String, PartitionKey>> itemKeyList, String collectionLink, CosmosQueryRequestOptions options, Class<T> klass) {
		HashMap<String, Object> attributes = new HashMap<String, Object>();
		Utils.addAttribute(attributes, "CollectionLink", collectionLink);
		Utils.addAttribute(attributes, "ItemType", klass.getName());
		String collectionName = CosmosUtils.getIDFromLink(collectionLink);
		
		Mono<FeedResponse<T>> result = Weaver.callOriginal();
		DatastoreParameters params = DatastoreParameters.product(CosmosUtils.COSMOSDB).collection(collectionName).operation("readMany").build();
		CompletionRunnable<Subscription> runnable = new CompletionRunnable<Subscription>("RxDocumentClientImpl/readMany", params, NewRelic.getAgent().getTransaction());
		runnable.setAttributes(attributes);
		ErrorConsumer errorConsumer = new ErrorConsumer(runnable);
		return result.doOnSubscribe(runnable).doOnTerminate(runnable).doOnCancel(runnable).doOnError(errorConsumer);
	}
	
}
