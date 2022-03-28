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
import com.newrelic.instrumentation.azure.cosmos.CompletionRunnable;
import com.newrelic.instrumentation.azure.cosmos.CosmosUtils;
import com.newrelic.instrumentation.azure.cosmos.ErrorConsumer;
import com.newrelic.instrumentation.azure.cosmos.Utils;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Weave
public abstract class RxDocumentClientImpl {


	public Mono<ResourceResponse<DocumentCollection>> createCollection(String databaseLink,DocumentCollection collection, RequestOptions options) {

		HashMap<String,Object> attributes = new HashMap<String, Object>();

		Utils.addAttribute(attributes, "Operation", "CreateCollection");
		Utils.addAttribute(attributes, "DatabaseLink", databaseLink);
		Utils.addDocumentCollection(attributes, collection);


		DatastoreParameters params = DatastoreParameters.product(CosmosUtils.COSMOSDB).collection(collection.getId()).operation("createCollection").build();
		CompletionRunnable<Subscription> runnable = new CompletionRunnable<Subscription>("RxDocumentClientImpl/createCollection", params, NewRelic.getAgent().getTransaction());
		runnable.setAttributes(attributes);
		Mono<ResourceResponse<DocumentCollection>> result = Weaver.callOriginal();
		ErrorConsumer errorConsumer = new ErrorConsumer(runnable);

		return result.doOnSubscribe(runnable).doOnCancel(runnable).doOnTerminate(runnable).doOnError(errorConsumer);
	}

	public Mono<ResourceResponse<Database>> createDatabase(Database database, RequestOptions options) {
		HashMap<String,Object> attributes = new HashMap<String, Object>();
		Utils.addAttribute(attributes, "Operation", "CreateDatabase");
		Utils.addDatabase(attributes, database);
		DatastoreParameters params = DatastoreParameters.product(CosmosUtils.COSMOSDB).collection(database.getId()).operation("createDatabase").build();
		CompletionRunnable<Subscription> runnable = new CompletionRunnable<Subscription>("RxDocumentClientImpl/createDatabase", params, NewRelic.getAgent().getTransaction());
		runnable.setAttributes(attributes);
		Mono<ResourceResponse<Database>> result = Weaver.callOriginal();
		ErrorConsumer errorConsumer = new ErrorConsumer(runnable);


		return result.doOnSubscribe(runnable).doOnCancel(runnable).doOnTerminate(runnable).doOnError(errorConsumer);
	}

	public Mono<ResourceResponse<Document>> createDocument(String collectionLink, Object document,RequestOptions options, boolean disableAutomaticIdGeneration) {

		HashMap<String,Object> attributes = new HashMap<String, Object>();
		Utils.addAttribute(attributes, "Operation", "CreateDocument");
		Utils.addAttribute(attributes, "CollectionLink", collectionLink);
		Utils.addAttribute(attributes, "DocumentType", document.getClass().getSimpleName());

		DatastoreParameters params = DatastoreParameters.product(CosmosUtils.COSMOSDB).collection(document.getClass().getSimpleName()).operation("createDocument").build();
		CompletionRunnable<Subscription> runnable = new CompletionRunnable<Subscription>("RxDocumentClientImpl/createDatabase", params, NewRelic.getAgent().getTransaction());
		runnable.setAttributes(attributes);

		Mono<ResourceResponse<Document>> result = Weaver.callOriginal();
		ErrorConsumer errorConsumer = new ErrorConsumer(runnable);


		return result.doOnSubscribe(runnable).doOnCancel(runnable).doOnTerminate(runnable).doOnError(errorConsumer);
	}

	public Mono<ResourceResponse<StoredProcedure>> createStoredProcedure(String collectionLink, StoredProcedure storedProcedure, RequestOptions options) {
		HashMap<String,Object> attributes = new HashMap<String, Object>();
		Utils.addAttribute(attributes, "Operation", "CreateStoredProcedure");
		Utils.addAttribute(attributes, "CollectionLink", collectionLink);
		Utils.addStoredProcedure(attributes, storedProcedure);

		DatastoreParameters params = DatastoreParameters.product(CosmosUtils.COSMOSDB).collection(storedProcedure.getId()).operation("createStoredProcedure").build();
		CompletionRunnable<Subscription> runnable = new CompletionRunnable<Subscription>("RxDocumentClientImpl/createStoredProcedure", params, NewRelic.getAgent().getTransaction());
		runnable.setAttributes(attributes);

		Mono<ResourceResponse<StoredProcedure>> result = Weaver.callOriginal();
		ErrorConsumer errorConsumer = new ErrorConsumer(runnable);


		return result.doOnSubscribe(runnable).doOnCancel(runnable).doOnTerminate(runnable).doOnError(errorConsumer);
	}

	public Mono<ResourceResponse<User>> createUser(String databaseLink, User user, RequestOptions options) {
		HashMap<String,Object> attributes = new HashMap<String, Object>();
		Utils.addAttribute(attributes, "Operation", "CreateUser");
		Utils.addAttribute(attributes, "DatabaseLink", databaseLink);
		Utils.addUser(attributes, user);

		DatastoreParameters params = DatastoreParameters.product(CosmosUtils.COSMOSDB).collection(CosmosUtils.getIDFromLink(databaseLink)).operation("createUser").build();
		CompletionRunnable<Subscription> runnable = new CompletionRunnable<Subscription>("RxDocumentClientImpl/createUser", params, NewRelic.getAgent().getTransaction());
		runnable.setAttributes(attributes);
		Mono<ResourceResponse<User>> result = Weaver.callOriginal();
		ErrorConsumer errorConsumer = new ErrorConsumer(runnable);


		return result.doOnSubscribe(runnable).doOnCancel(runnable).doOnTerminate(runnable).doOnError(errorConsumer);
	}

	public Mono<ResourceResponse<DocumentCollection>> deleteCollection(String collectionLink,RequestOptions options) {
		HashMap<String,Object> attributes = new HashMap<String, Object>();
		Utils.addAttribute(attributes, "Operation", "DeleteCollection");
		Utils.addAttribute(attributes, "CollectionLink", collectionLink);

		DatastoreParameters params = DatastoreParameters.product(CosmosUtils.COSMOSDB).collection(CosmosUtils.getIDFromLink(collectionLink)).operation("deleteAllDocumentsByPartitionKey").build();
		CompletionRunnable<Subscription> runnable = new CompletionRunnable<Subscription>("RxDocumentClientImpl/deleteAllDocumentsByPartitionKey", params, NewRelic.getAgent().getTransaction());
		runnable.setAttributes(attributes);

		Mono<ResourceResponse<DocumentCollection>> result = Weaver.callOriginal();
		ErrorConsumer errorConsumer = new ErrorConsumer(runnable);


		return result.doOnSubscribe(runnable).doOnCancel(runnable).doOnTerminate(runnable).doOnError(errorConsumer);
	}

	public Mono<ResourceResponse<Database>> deleteDatabase(String databaseLink, RequestOptions options) {
		HashMap<String,Object> attributes = new HashMap<String, Object>();
		Utils.addAttribute(attributes, "Operation", "DeleteDatabase");
		Utils.addAttribute(attributes, "DatabaseLink", databaseLink);

		DatastoreParameters params = DatastoreParameters.product(CosmosUtils.COSMOSDB).collection(CosmosUtils.getIDFromLink(databaseLink)).operation("deleteDatabase").build();
		CompletionRunnable<Subscription> runnable = new CompletionRunnable<Subscription>("RxDocumentClientImpl/deleteDatabase", params, NewRelic.getAgent().getTransaction());
		runnable.setAttributes(attributes);

		Mono<ResourceResponse<Database>> result = Weaver.callOriginal();
		ErrorConsumer errorConsumer = new ErrorConsumer(runnable);


		return result.doOnSubscribe(runnable).doOnCancel(runnable).doOnTerminate(runnable).doOnError(errorConsumer);
	}

	public Mono<ResourceResponse<Document>> deleteDocument(String documentLink, RequestOptions options) {
		HashMap<String,Object> attributes = new HashMap<String, Object>();
		Utils.addAttribute(attributes, "Operation", "DeleteDocument");
		Utils.addAttribute(attributes, "DocumentLink", documentLink);
		DatastoreParameters params = DatastoreParameters.product(CosmosUtils.COSMOSDB).collection(CosmosUtils.getIDFromLink(documentLink)).operation("deleteDocument").build();
		CompletionRunnable<Subscription> runnable = new CompletionRunnable<Subscription>("RxDocumentClientImpl/deleteDocument", params, NewRelic.getAgent().getTransaction());
		runnable.setAttributes(attributes);

		Mono<ResourceResponse<Document>> result = Weaver.callOriginal();
		ErrorConsumer errorConsumer = new ErrorConsumer(runnable);


		return result.doOnSubscribe(runnable).doOnCancel(runnable).doOnTerminate(runnable).doOnError(errorConsumer);
	}

	public Mono<ResourceResponse<StoredProcedure>> deleteStoredProcedure(String storedProcedureLink, RequestOptions options) {
		HashMap<String,Object> attributes = new HashMap<String, Object>();
		Utils.addAttribute(attributes, "Operation", "DeleteStoredProcedure");
		Utils.addAttribute(attributes, "StoredProcedureLink", storedProcedureLink);
		DatastoreParameters params = DatastoreParameters.product(CosmosUtils.COSMOSDB).collection(CosmosUtils.getIDFromLink(storedProcedureLink)).operation("deleteStoredProcedure").build();
		CompletionRunnable<Subscription> runnable = new CompletionRunnable<Subscription>("RxDocumentClientImpl/deleteStoredProcedure", params, NewRelic.getAgent().getTransaction());
		runnable.setAttributes(attributes);

		Mono<ResourceResponse<StoredProcedure>> result = Weaver.callOriginal();
		ErrorConsumer errorConsumer = new ErrorConsumer(runnable);


		return result.doOnSubscribe(runnable).doOnCancel(runnable).doOnTerminate(runnable).doOnError(errorConsumer);
	}

	public Mono<ResourceResponse<User>> deleteUser(String userLink, RequestOptions options) {
		HashMap<String,Object> attributes = new HashMap<String, Object>();
		Utils.addAttribute(attributes, "Operation", "DeleteStoredProcedure");
		Utils.addAttribute(attributes, "UserLink", userLink);
		DatastoreParameters params = DatastoreParameters.product(CosmosUtils.COSMOSDB).collection("").operation("deleteUser").build();
		CompletionRunnable<Subscription> runnable = new CompletionRunnable<Subscription>("RxDocumentClientImpl/deleteUser", params, NewRelic.getAgent().getTransaction());
		runnable.setAttributes(attributes);

		Mono<ResourceResponse<User>> result = Weaver.callOriginal();
		ErrorConsumer errorConsumer = new ErrorConsumer(runnable);


		return result.doOnSubscribe(runnable).doOnCancel(runnable).doOnTerminate(runnable).doOnError(errorConsumer);
	}

	public Mono<StoredProcedureResponse> executeStoredProcedure(String storedProcedureLink, RequestOptions options, List<Object> procedureParams) {
		HashMap<String,Object> attributes = new HashMap<String, Object>();
		Utils.addAttribute(attributes, "Operation", "ExecuteStoredProcedure");
		Utils.addAttribute(attributes, "StoredProcedureLink", storedProcedureLink);
		DatastoreParameters params = DatastoreParameters.product(CosmosUtils.COSMOSDB).collection(CosmosUtils.getIDFromLink(storedProcedureLink)).operation("executeStoredProcedure").build();
		CompletionRunnable<Subscription> runnable = new CompletionRunnable<Subscription>("RxDocumentClientImpl/executeStoredProcedure", params, NewRelic.getAgent().getTransaction());
		runnable.setAttributes(attributes);

		Mono<StoredProcedureResponse> result = Weaver.callOriginal();
		ErrorConsumer errorConsumer = new ErrorConsumer(runnable);


		return result.doOnSubscribe(runnable).doOnCancel(runnable).doOnTerminate(runnable).doOnError(errorConsumer);
	}

	public Flux<FeedResponse<DocumentCollection>> queryCollections(String databaseLink,SqlQuerySpec querySpec, CosmosQueryRequestOptions options) {
		HashMap<String,Object> attributes = new HashMap<String, Object>();
		Utils.addAttribute(attributes, "Operation", "QueryCollections");
		Utils.addAttribute(attributes, "DatabaseLink", databaseLink);
		String sql = querySpec.getQueryText();
		Utils.addAttribute(attributes, "Query", sql);
		ParsedDatabaseStatement statement = CosmosUtils.parseSQL(querySpec.getQueryText());
		DatastoreParameters params = null;
		if(statement != null) {
			params = DatastoreParameters.product(CosmosUtils.COSMOSDB).collection(statement.getModel()).operation(statement.getOperation()).noInstance().noDatabaseName().slowQuery(sql, Utils.getConverter()).build();
		}

		CompletionRunnable<Subscription> runnable = new CompletionRunnable<Subscription>("CosmosAsyncClient/query", params, NewRelic.getAgent().getTransaction());
		ErrorConsumer errorConsumer = new ErrorConsumer(runnable);
		Flux<FeedResponse<DocumentCollection>>  result = Weaver.callOriginal();

		return result.doOnSubscribe(runnable).doOnCancel(runnable).doOnComplete(runnable).doOnError(errorConsumer);
	}

	public Flux<FeedResponse<DocumentCollection>> queryCollections(String databaseLink, String query,CosmosQueryRequestOptions options)  {
		HashMap<String,Object> attributes = new HashMap<String, Object>();
		Utils.addAttribute(attributes, "Operation", "QueryCollections");
		Utils.addAttribute(attributes, "DatabaseLink", databaseLink);
		Utils.addAttribute(attributes, "Query", query);

		ParsedDatabaseStatement statement = CosmosUtils.parseSQL(query);
		DatastoreParameters params = null;
		if(statement != null) {
			params = DatastoreParameters.product(CosmosUtils.COSMOSDB).collection(statement.getModel()).operation(statement.getOperation()).noInstance().noDatabaseName().slowQuery(query, Utils.getConverter()).build();
		}

		CompletionRunnable<Subscription> runnable = new CompletionRunnable<Subscription>("CosmosAsyncClient/query", params, NewRelic.getAgent().getTransaction());
		runnable.setAttributes(attributes);

		ErrorConsumer errorConsumer = new ErrorConsumer(runnable);
		Flux<FeedResponse<DocumentCollection>>  result = Weaver.callOriginal();

		return result.doOnSubscribe(runnable).doOnCancel(runnable).doOnComplete(runnable).doOnError(errorConsumer);
	}

	public Flux<FeedResponse<Conflict>> queryConflicts(String collectionLink, SqlQuerySpec querySpec,
			CosmosQueryRequestOptions options) {
		HashMap<String,Object> attributes = new HashMap<String, Object>();
		Utils.addAttribute(attributes, "Operation", "QueryConflicts");
		Utils.addAttribute(attributes, "CollectionLink", collectionLink);
		String query = querySpec.getQueryText();
		Utils.addAttribute(attributes, "Query", query);

		ParsedDatabaseStatement statement = CosmosUtils.parseSQL(query);
		DatastoreParameters params = null;
		if(statement != null) {
			params = DatastoreParameters.product(CosmosUtils.COSMOSDB).collection(statement.getModel()).operation(statement.getOperation()).noInstance().noDatabaseName().slowQuery(query, Utils.getConverter()).build();
		}

		CompletionRunnable<Subscription> runnable = new CompletionRunnable<Subscription>("CosmosAsyncClient/query", params, NewRelic.getAgent().getTransaction());
		runnable.setAttributes(attributes);

		ErrorConsumer errorConsumer = new ErrorConsumer(runnable);
		Flux<FeedResponse<Conflict>>  result = Weaver.callOriginal();

		return result.doOnSubscribe(runnable).doOnCancel(runnable).doOnComplete(runnable).doOnError(errorConsumer);
	}

	public Flux<FeedResponse<Database>> queryDatabases(SqlQuerySpec querySpec, CosmosQueryRequestOptions options) {
		HashMap<String,Object> attributes = new HashMap<String, Object>();
		Utils.addAttribute(attributes, "Operation", "QueryDatabases");
		String query = querySpec.getQueryText();
		Utils.addAttribute(attributes, "Query", query);

		ParsedDatabaseStatement statement = CosmosUtils.parseSQL(query);
		DatastoreParameters params = null;
		if(statement != null) {
			params = DatastoreParameters.product(CosmosUtils.COSMOSDB).collection(statement.getModel()).operation(statement.getOperation()).noInstance().noDatabaseName().slowQuery(query, Utils.getConverter()).build();
		}

		CompletionRunnable<Subscription> runnable = new CompletionRunnable<Subscription>("CosmosAsyncClient/query", params, NewRelic.getAgent().getTransaction());
		runnable.setAttributes(attributes);

		ErrorConsumer errorConsumer = new ErrorConsumer(runnable);
		Flux<FeedResponse<Database>>  result = Weaver.callOriginal();

		return result.doOnSubscribe(runnable).doOnCancel(runnable).doOnComplete(runnable).doOnError(errorConsumer);
	}

	public Flux<FeedResponse<Document>> queryDocuments(String collectionLink, SqlQuerySpec querySpec,
			CosmosQueryRequestOptions options) {
		HashMap<String,Object> attributes = new HashMap<String, Object>();
		Utils.addAttribute(attributes, "Operation", "QueryDocuments");
		String query = querySpec.getQueryText();
		Utils.addAttribute(attributes, "Query", query);

		ParsedDatabaseStatement statement = CosmosUtils.parseSQL(query);
		DatastoreParameters params = null;
		if(statement != null) {
			params = DatastoreParameters.product(CosmosUtils.COSMOSDB).collection(statement.getModel()).operation(statement.getOperation()).noInstance().noDatabaseName().slowQuery(query, Utils.getConverter()).build();
		}

		CompletionRunnable<Subscription> runnable = new CompletionRunnable<Subscription>("CosmosAsyncClient/query", params, NewRelic.getAgent().getTransaction());
		runnable.setAttributes(attributes);

		ErrorConsumer errorConsumer = new ErrorConsumer(runnable);
		Flux<FeedResponse<Document>>  result = Weaver.callOriginal();

		return result.doOnSubscribe(runnable).doOnCancel(runnable).doOnComplete(runnable).doOnError(errorConsumer);
	}

	public Flux<FeedResponse<Offer>> queryOffers(SqlQuerySpec querySpec, CosmosQueryRequestOptions options) {
		HashMap<String,Object> attributes = new HashMap<String, Object>();
		Utils.addAttribute(attributes, "Operation", "QueryOffers");
		String query = querySpec.getQueryText();
		Utils.addAttribute(attributes, "Query", query);

		ParsedDatabaseStatement statement = CosmosUtils.parseSQL(query);
		DatastoreParameters params = null;
		if(statement != null) {
			params = DatastoreParameters.product(CosmosUtils.COSMOSDB).collection(statement.getModel()).operation(statement.getOperation()).noInstance().noDatabaseName().slowQuery(query, Utils.getConverter()).build();
		}

		CompletionRunnable<Subscription> runnable = new CompletionRunnable<Subscription>("CosmosAsyncClient/query", params, NewRelic.getAgent().getTransaction());
		runnable.setAttributes(attributes);

		ErrorConsumer errorConsumer = new ErrorConsumer(runnable);
		Flux<FeedResponse<Offer>>  result = Weaver.callOriginal();

		return result.doOnSubscribe(runnable).doOnCancel(runnable).doOnComplete(runnable).doOnError(errorConsumer);
	}
	
    public Flux<FeedResponse<StoredProcedure>> queryStoredProcedures(String collectionLink,
            SqlQuerySpec querySpec, CosmosQueryRequestOptions options) {
		HashMap<String,Object> attributes = new HashMap<String, Object>();
		Utils.addAttribute(attributes, "Operation", "QueryStoredProcedures");
		String query = querySpec.getQueryText();
		Utils.addAttribute(attributes, "Query", query);

		ParsedDatabaseStatement statement = CosmosUtils.parseSQL(query);
		DatastoreParameters params = null;
		if(statement != null) {
			params = DatastoreParameters.product(CosmosUtils.COSMOSDB).collection(statement.getModel()).operation(statement.getOperation()).noInstance().noDatabaseName().slowQuery(query, Utils.getConverter()).build();
		}

		CompletionRunnable<Subscription> runnable = new CompletionRunnable<Subscription>("CosmosAsyncClient/query", params, NewRelic.getAgent().getTransaction());
		runnable.setAttributes(attributes);

		ErrorConsumer errorConsumer = new ErrorConsumer(runnable);
		Flux<FeedResponse<StoredProcedure>>  result = Weaver.callOriginal();

		return result.doOnSubscribe(runnable).doOnCancel(runnable).doOnComplete(runnable).doOnError(errorConsumer);
    }

    public Flux<FeedResponse<User>> queryUsers(String databaseLink, SqlQuerySpec querySpec,
            CosmosQueryRequestOptions options) {
		HashMap<String,Object> attributes = new HashMap<String, Object>();
		Utils.addAttribute(attributes, "Operation", "QueryUsers");
		String query = querySpec.getQueryText();
		Utils.addAttribute(attributes, "Query", query);

		ParsedDatabaseStatement statement = CosmosUtils.parseSQL(query);
		DatastoreParameters params = null;
		if(statement != null) {
			params = DatastoreParameters.product(CosmosUtils.COSMOSDB).collection(statement.getModel()).operation(statement.getOperation()).noInstance().noDatabaseName().slowQuery(query, Utils.getConverter()).build();
		}

		CompletionRunnable<Subscription> runnable = new CompletionRunnable<Subscription>("CosmosAsyncClient/query", params, NewRelic.getAgent().getTransaction());
		runnable.setAttributes(attributes);

		ErrorConsumer errorConsumer = new ErrorConsumer(runnable);
		Flux<FeedResponse<User>>  result = Weaver.callOriginal();

		return result.doOnSubscribe(runnable).doOnCancel(runnable).doOnComplete(runnable).doOnError(errorConsumer);
    }

	public Mono<ResourceResponse<DocumentCollection>> readCollection(String collectionLink, RequestOptions options) {
		HashMap<String,Object> attributes = new HashMap<String, Object>();
		Utils.addAttribute(attributes, "Operation", "ReadCollection");
		Utils.addAttribute(attributes, "CollectionLink", collectionLink);
		DatastoreParameters params = DatastoreParameters.product(CosmosUtils.COSMOSDB).collection(CosmosUtils.getIDFromLink(collectionLink)).operation("readCollection").build();
		CompletionRunnable<Subscription> runnable = new CompletionRunnable<Subscription>("RxDocumentClientImpl/readCollection", params, NewRelic.getAgent().getTransaction());
		runnable.setAttributes(attributes);

		Mono<ResourceResponse<DocumentCollection>> result = Weaver.callOriginal();
		ErrorConsumer errorConsumer = new ErrorConsumer(runnable);


		return result.doOnSubscribe(runnable).doOnCancel(runnable).doOnTerminate(runnable).doOnError(errorConsumer);
	}


	public Flux<FeedResponse<DocumentCollection>> readCollections(String databaseLink, CosmosQueryRequestOptions options) {
		HashMap<String,Object> attributes = new HashMap<String, Object>();
		Utils.addAttribute(attributes, "Operation", "ReadCollections");
		Utils.addAttribute(attributes, "DatabaseLink", databaseLink);

		DatastoreParameters params = DatastoreParameters.product(CosmosUtils.COSMOSDB).collection(CosmosUtils.getIDFromLink(databaseLink)).operation("readCollections").build();
		CompletionRunnable<Subscription> runnable = new CompletionRunnable<Subscription>("RxDocumentClientImpl/readCollections", params, NewRelic.getAgent().getTransaction());
		runnable.setAttributes(attributes);

		ErrorConsumer errorConsumer = new ErrorConsumer(runnable);

		Flux<FeedResponse<DocumentCollection>>  result = Weaver.callOriginal();

		return result.doOnSubscribe(runnable).doOnCancel(runnable).doOnComplete(runnable).doOnError(errorConsumer);
	}

	public Mono<ResourceResponse<Database>> readDatabase(String databaseLink, RequestOptions options) {
		HashMap<String,Object> attributes = new HashMap<String, Object>();
		Utils.addAttribute(attributes, "Operation", "ReadDatabase");
		Utils.addAttribute(attributes, "DatabaseLink", databaseLink);
		DatastoreParameters params = DatastoreParameters.product(CosmosUtils.COSMOSDB).collection(CosmosUtils.getIDFromLink(databaseLink)).operation("readDatabase").build();
		CompletionRunnable<Subscription> runnable = new CompletionRunnable<Subscription>("RxDocumentClientImpl/readDatabase", params, NewRelic.getAgent().getTransaction());
		runnable.setAttributes(attributes);

		Mono<ResourceResponse<Database>> result = Weaver.callOriginal();
		ErrorConsumer errorConsumer = new ErrorConsumer(runnable);


		return result.doOnSubscribe(runnable).doOnCancel(runnable).doOnTerminate(runnable).doOnError(errorConsumer);
	}

	public Flux<FeedResponse<Database>> readDatabases(CosmosQueryRequestOptions options) {
		HashMap<String,Object> attributes = new HashMap<String, Object>();
		Utils.addAttribute(attributes, "Operation", "ReadDatabases");
		DatastoreParameters params = DatastoreParameters.product(CosmosUtils.COSMOSDB).collection("").operation("readDatabases").build();
		CompletionRunnable<Subscription> runnable = new CompletionRunnable<Subscription>("RxDocumentClientImpl/readDatabases", params, NewRelic.getAgent().getTransaction());
		runnable.setAttributes(attributes);

		Flux<FeedResponse<Database>> result = Weaver.callOriginal();
		ErrorConsumer errorConsumer = new ErrorConsumer(runnable);


		return result.doOnSubscribe(runnable).doOnCancel(runnable).doOnTerminate(runnable).doOnError(errorConsumer);
	}
	
	public Flux<FeedResponse<Document>> readDocuments(String collectionLink, CosmosQueryRequestOptions options) {
		HashMap<String,Object> attributes = new HashMap<String, Object>();
		Utils.addAttribute(attributes, "Operation", "ReadDocuments");
		DatastoreParameters params = DatastoreParameters.product(CosmosUtils.COSMOSDB).collection(CosmosUtils.getIDFromLink(collectionLink)).operation("readDocuments").build();
		CompletionRunnable<Subscription> runnable = new CompletionRunnable<Subscription>("RxDocumentClientImpl/readDocuments", params, NewRelic.getAgent().getTransaction());
		runnable.setAttributes(attributes);

		Flux<FeedResponse<Document>> result = Weaver.callOriginal();
		ErrorConsumer errorConsumer = new ErrorConsumer(runnable);


		return result.doOnSubscribe(runnable).doOnCancel(runnable).doOnTerminate(runnable).doOnError(errorConsumer);
	}
	
	public <T> Mono<FeedResponse<T>> readMany(
			List<Pair<String, PartitionKey>> itemKeyList,
			String collectionLink,
			CosmosQueryRequestOptions options,
			Class<T> klass) {

		HashMap<String,Object> attributes = new HashMap<String, Object>();
		Utils.addAttribute(attributes, "Operation", "ReadMany");
		Utils.addAttribute(attributes, "CollectionsLink", collectionLink);
		for(Pair<String, PartitionKey> itemKey : itemKeyList) {
			Utils.addAttribute(attributes, "PartitionKey-"+itemKey.getKey(), itemKey.getValue().toString());
		}
		Utils.addAttribute(attributes, "Class", klass.getSimpleName());

		DatastoreParameters params = DatastoreParameters.product(CosmosUtils.COSMOSDB).collection(CosmosUtils.getIDFromLink(collectionLink)).operation("readMany").build();
		CompletionRunnable<Subscription> runnable = new CompletionRunnable<Subscription>("RxDocumentClientImpl/readMany", params, NewRelic.getAgent().getTransaction());
		runnable.setAttributes(attributes);

		Mono<FeedResponse<T>> result = Weaver.callOriginal();
		ErrorConsumer errorConsumer = new ErrorConsumer(runnable);


		return result.doOnSubscribe(runnable).doOnCancel(runnable).doOnTerminate(runnable).doOnError(errorConsumer);
	}

	public Mono<ResourceResponse<Offer>> readOffer(String offerLink) {
		HashMap<String,Object> attributes = new HashMap<String, Object>();
		Utils.addAttribute(attributes, "Operation", "ReadMany");
		Utils.addAttribute(attributes, "OfferLink", offerLink);
		DatastoreParameters params = DatastoreParameters.product(CosmosUtils.COSMOSDB).collection(CosmosUtils.getIDFromLink(offerLink)).operation("readOffer").build();
		CompletionRunnable<Subscription> runnable = new CompletionRunnable<Subscription>("RxDocumentClientImpl/readOffer", params, NewRelic.getAgent().getTransaction());
		runnable.setAttributes(attributes);

		Mono<ResourceResponse<Offer>> result = Weaver.callOriginal();
		ErrorConsumer errorConsumer = new ErrorConsumer(runnable);


		return result.doOnSubscribe(runnable).doOnCancel(runnable).doOnTerminate(runnable).doOnError(errorConsumer);
	}

	public Flux<FeedResponse<Offer>> readOffers(CosmosQueryRequestOptions options) {
		DatastoreParameters params = DatastoreParameters.product(CosmosUtils.COSMOSDB).collection("").operation("readOffers").build();
		CompletionRunnable<Subscription> runnable = new CompletionRunnable<Subscription>("RxDocumentClientImpl/readOffers", params, NewRelic.getAgent().getTransaction());
		ErrorConsumer errorConsumer = new ErrorConsumer(runnable);


		Flux<FeedResponse<Offer>> result = Weaver.callOriginal();

		return result.doOnSubscribe(runnable).doOnCancel(runnable).doOnComplete(runnable).doOnError(errorConsumer);
	}

	public Mono<ResourceResponse<StoredProcedure>> readStoredProcedure(String storedProcedureLink, RequestOptions options) {
		HashMap<String,Object> attributes = new HashMap<String, Object>();
		Utils.addAttribute(attributes, "Operation", "ReadMany");
		Utils.addAttribute(attributes, "StoredProcedureLink", storedProcedureLink);
		DatastoreParameters params = DatastoreParameters.product(CosmosUtils.COSMOSDB).collection(CosmosUtils.getIDFromLink(storedProcedureLink)).operation("readStoredProcedure").build();
		CompletionRunnable<Subscription> runnable = new CompletionRunnable<Subscription>("RxDoc	umentClientImpl/readStoredProcedure", params, NewRelic.getAgent().getTransaction());
		runnable.setAttributes(attributes);

		ErrorConsumer errorConsumer = new ErrorConsumer(runnable);

		Mono<ResourceResponse<StoredProcedure>> result = Weaver.callOriginal();

		return result.doOnSubscribe(runnable).doOnCancel(runnable).doOnTerminate(runnable).doOnError(errorConsumer);
	}

	public Flux<FeedResponse<StoredProcedure>> readStoredProcedures(String collectionLink, CosmosQueryRequestOptions options) {
		HashMap<String,Object> attributes = new HashMap<String, Object>();
		Utils.addAttribute(attributes, "Operation", "ReadStoredProcedures");
		Utils.addAttribute(attributes, "CollectionsLink", collectionLink);
		DatastoreParameters params = DatastoreParameters.product(CosmosUtils.COSMOSDB).collection(CosmosUtils.getIDFromLink(collectionLink)).operation("readStoredProcedures").build();
		CompletionRunnable<Subscription> runnable = new CompletionRunnable<Subscription>("RxDocumentClientImpl/readStoredProcedures", params, NewRelic.getAgent().getTransaction());
		runnable.setAttributes(attributes);

		ErrorConsumer errorConsumer = new ErrorConsumer(runnable);

		Flux<FeedResponse<StoredProcedure>> result = Weaver.callOriginal();

		return result.doOnSubscribe(runnable).doOnCancel(runnable).doOnTerminate(runnable).doOnError(errorConsumer);
	}

	public Mono<ResourceResponse<User>> readUser(String userLink, RequestOptions options) {
		HashMap<String,Object> attributes = new HashMap<String, Object>();
		Utils.addAttribute(attributes, "Operation", "readUser");
		Utils.addAttribute(attributes, "UserLink", userLink);
		DatastoreParameters params = DatastoreParameters.product(CosmosUtils.COSMOSDB).collection("").operation("readUser").build();
		CompletionRunnable<Subscription> runnable = new CompletionRunnable<Subscription>("RxDocumentClientImpl/readUser", params, NewRelic.getAgent().getTransaction());
		runnable.setAttributes(attributes);

		ErrorConsumer errorConsumer = new ErrorConsumer(runnable);

		Mono<ResourceResponse<User>> result = Weaver.callOriginal();

		return result.doOnSubscribe(runnable).doOnCancel(runnable).doOnTerminate(runnable).doOnError(errorConsumer);
	}

	public Flux<FeedResponse<User>> readUsers(String databaseLink, CosmosQueryRequestOptions options) {
		HashMap<String,Object> attributes = new HashMap<String, Object>();
		Utils.addAttribute(attributes, "Operation", "readUsers");
		Utils.addAttribute(attributes, "DatabaseLink", databaseLink);
		DatastoreParameters params = DatastoreParameters.product(CosmosUtils.COSMOSDB).collection(CosmosUtils.getIDFromLink(databaseLink)).operation("readUsers").build();
		CompletionRunnable<Subscription> runnable = new CompletionRunnable<Subscription>("RxDocumentClientImpl/readStoredreadUsersProcedures", params, NewRelic.getAgent().getTransaction());
		runnable.setAttributes(attributes);

		ErrorConsumer errorConsumer = new ErrorConsumer(runnable);

		Flux<FeedResponse<User>> result = Weaver.callOriginal();

		return result.doOnSubscribe(runnable).doOnCancel(runnable).doOnTerminate(runnable).doOnError(errorConsumer);
	}

	public Mono<ResourceResponse<DocumentCollection>> replaceCollection(DocumentCollection collection, RequestOptions options) {
		HashMap<String,Object> attributes = new HashMap<String, Object>();
		Utils.addAttribute(attributes, "Operation", "ReplaceCollection");
		Utils.addDocumentCollection(attributes, collection);
		DatastoreParameters params = DatastoreParameters.product(CosmosUtils.COSMOSDB).collection(collection.getId()).operation("replaceCollection").build();
		CompletionRunnable<Subscription> runnable = new CompletionRunnable<Subscription>("RxDocumentClientImpl/replaceCollection", params, NewRelic.getAgent().getTransaction());
		runnable.setAttributes(attributes);

		ErrorConsumer errorConsumer = new ErrorConsumer(runnable);

		Mono<ResourceResponse<DocumentCollection>> result = Weaver.callOriginal();

		return result.doOnSubscribe(runnable).doOnCancel(runnable).doOnTerminate(runnable).doOnError(errorConsumer);
	}

	public Mono<ResourceResponse<Document>> replaceDocument(Document document, RequestOptions options) {
		HashMap<String,Object> attributes = new HashMap<String, Object>();
		Utils.addAttribute(attributes, "Operation", "ReplaceDocument");
		Utils.addDocument(attributes, document);
		DatastoreParameters params = DatastoreParameters.product(CosmosUtils.COSMOSDB).collection(document.getId()).operation("replaceDocument").build();
		CompletionRunnable<Subscription> runnable = new CompletionRunnable<Subscription>("RxDocumentClientImpl/replaceDocument", params, NewRelic.getAgent().getTransaction());
		runnable.setAttributes(attributes);

		ErrorConsumer errorConsumer = new ErrorConsumer(runnable);

		Mono<ResourceResponse<Document>> result = Weaver.callOriginal();

		return result.doOnSubscribe(runnable).doOnCancel(runnable).doOnTerminate(runnable).doOnError(errorConsumer);
	}

	public Mono<ResourceResponse<Document>> replaceDocument(String documentLink, Object document, RequestOptions options) {
		HashMap<String,Object> attributes = new HashMap<String, Object>();
		Utils.addAttribute(attributes, "Operation", "ReplaceDocument");
		Utils.addAttribute(attributes, "DocumentLink", documentLink);
		Utils.addAttribute(attributes, "DocumentType", document.getClass().getSimpleName());

		DatastoreParameters params = DatastoreParameters.product(CosmosUtils.COSMOSDB).collection(CosmosUtils.getIDFromLink(documentLink)).operation("replaceDocument").build();
		CompletionRunnable<Subscription> runnable = new CompletionRunnable<Subscription>("RxDocumentClientImpl/replaceDocument", params, NewRelic.getAgent().getTransaction());
		ErrorConsumer errorConsumer = new ErrorConsumer(runnable);

		Mono<ResourceResponse<Document>> result = Weaver.callOriginal();

		return result.doOnSubscribe(runnable).doOnCancel(runnable).doOnTerminate(runnable).doOnError(errorConsumer);
	}

	public Mono<ResourceResponse<Offer>> replaceOffer(Offer offer) {
		HashMap<String,Object> attributes = new HashMap<String, Object>();
		Utils.addAttribute(attributes, "Operation", "ReplaceOffer");
		Utils.addOffer(attributes, offer);

		DatastoreParameters params = DatastoreParameters.product(CosmosUtils.COSMOSDB).collection(offer.getId()).operation("replaceOffer").build();
		CompletionRunnable<Subscription> runnable = new CompletionRunnable<Subscription>("RxDocumentClientImpl/replaceOffer", params, NewRelic.getAgent().getTransaction());
		runnable.setAttributes(attributes);

		ErrorConsumer errorConsumer = new ErrorConsumer(runnable);

		Mono<ResourceResponse<Offer>> result = Weaver.callOriginal();

		return result.doOnSubscribe(runnable).doOnCancel(runnable).doOnTerminate(runnable).doOnError(errorConsumer);
	}

	public Mono<ResourceResponse<StoredProcedure>> replaceStoredProcedure(StoredProcedure storedProcedure, RequestOptions options) {
		HashMap<String,Object> attributes = new HashMap<String, Object>();
		Utils.addAttribute(attributes, "Operation", "ReplaceStoredProcedure");
		Utils.addStoredProcedure(attributes, storedProcedure);

		DatastoreParameters params = DatastoreParameters.product(CosmosUtils.COSMOSDB).collection(storedProcedure.getId()).operation("replaceStoredProcedure").build();
		CompletionRunnable<Subscription> runnable = new CompletionRunnable<Subscription>("RxDocumentClientImpl/replaceStoredProcedure", params, NewRelic.getAgent().getTransaction());
		runnable.setAttributes(attributes);

		ErrorConsumer errorConsumer = new ErrorConsumer(runnable);

		Mono<ResourceResponse<StoredProcedure>> result = Weaver.callOriginal();

		return result.doOnSubscribe(runnable).doOnCancel(runnable).doOnTerminate(runnable).doOnError(errorConsumer);
	}

	public Mono<ResourceResponse<User>> replaceUser(User user, RequestOptions options) {
		HashMap<String,Object> attributes = new HashMap<String, Object>();
		Utils.addAttribute(attributes, "Operation", "ReplaceUser");
		Utils.addUser(attributes, user);
		DatastoreParameters params = DatastoreParameters.product(CosmosUtils.COSMOSDB).collection("").operation("replaceUser").build();
		CompletionRunnable<Subscription> runnable = new CompletionRunnable<Subscription>("RxDocumentClientImpl/replaceUser", params, NewRelic.getAgent().getTransaction());
		runnable.setAttributes(attributes);

		ErrorConsumer errorConsumer = new ErrorConsumer(runnable);

		Mono<ResourceResponse<User>> result = Weaver.callOriginal();

		return result.doOnSubscribe(runnable).doOnCancel(runnable).doOnTerminate(runnable).doOnError(errorConsumer);
	}

	public Mono<ResourceResponse<Document>> upsertDocument(String collectionLink, Object document,
			RequestOptions options, boolean disableAutomaticIdGeneration) {
		HashMap<String,Object> attributes = new HashMap<String, Object>();
		Utils.addAttribute(attributes, "Operation", "UpsertDocument");
		Utils.addAttribute(attributes, "CollectionLink",collectionLink);
		Utils.addAttribute(attributes, "DocumentType",document.getClass().getSimpleName());

		DatastoreParameters params = DatastoreParameters.product(CosmosUtils.COSMOSDB).collection(CosmosUtils.getIDFromLink(collectionLink)).operation("upsertDocument").build();
		CompletionRunnable<Subscription> runnable = new CompletionRunnable<Subscription>("RxDocumentClientImpl/upsertDocument", params, NewRelic.getAgent().getTransaction());
		runnable.setAttributes(attributes);

		ErrorConsumer errorConsumer = new ErrorConsumer(runnable);

		Mono<ResourceResponse<Document>> result = Weaver.callOriginal();

		return result.doOnSubscribe(runnable).doOnCancel(runnable).doOnTerminate(runnable).doOnError(errorConsumer);
	}

	public Mono<ResourceResponse<StoredProcedure>> upsertStoredProcedure(String collectionLink,
			StoredProcedure storedProcedure, RequestOptions options) {
		HashMap<String,Object> attributes = new HashMap<String, Object>();
		Utils.addAttribute(attributes, "Operation", "UpsertStoredProcedure");
		Utils.addAttribute(attributes, "CollectionLink",collectionLink);
		Utils.addStoredProcedure(attributes, storedProcedure);

		DatastoreParameters params = DatastoreParameters.product(CosmosUtils.COSMOSDB).collection(CosmosUtils.getIDFromLink(collectionLink)).operation("upsertStoredProcedure").build();
		CompletionRunnable<Subscription> runnable = new CompletionRunnable<Subscription>("RxDocumentClientImpl/upsertStoredProcedure", params, NewRelic.getAgent().getTransaction());
		runnable.setAttributes(attributes);

		ErrorConsumer errorConsumer = new ErrorConsumer(runnable);

		Mono<ResourceResponse<StoredProcedure>> result = Weaver.callOriginal();

		return result.doOnSubscribe(runnable).doOnCancel(runnable).doOnTerminate(runnable).doOnError(errorConsumer);
	}

	public Mono<ResourceResponse<User>> upsertUser(String databaseLink, User user, RequestOptions options) {
		HashMap<String,Object> attributes = new HashMap<String, Object>();
		Utils.addAttribute(attributes, "Operation", "UpsertStoredProcedure");
		Utils.addAttribute(attributes, "DatabaseLink",databaseLink);
		Utils.addUser(attributes, user);

		DatastoreParameters params = DatastoreParameters.product(CosmosUtils.COSMOSDB).collection("").operation("upsertUser").build();
		CompletionRunnable<Subscription> runnable = new CompletionRunnable<Subscription>("RxDocumentClientImpl/upsertUser", params, NewRelic.getAgent().getTransaction());
		runnable.setAttributes(attributes);

		ErrorConsumer errorConsumer = new ErrorConsumer(runnable);

		Mono<ResourceResponse<User>> result = Weaver.callOriginal();

		return result.doOnSubscribe(runnable).doOnCancel(runnable).doOnTerminate(runnable).doOnError(errorConsumer);
	}

}
