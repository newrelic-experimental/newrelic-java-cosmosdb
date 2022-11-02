package com.azure.cosmos.implementation;

import java.util.HashMap;

import org.reactivestreams.Subscription;

import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.FeedResponse;
import com.azure.cosmos.models.PartitionKey;
import com.newrelic.api.agent.DatastoreParameters;
import com.newrelic.api.agent.NewRelic;
import com.newrelic.api.agent.weaver.Weave;
import com.newrelic.api.agent.weaver.Weaver;
import com.newrelic.instrumentation.azure.cosmos427.CompletionRunnable;
import com.newrelic.instrumentation.azure.cosmos427.CosmosUtils;
import com.newrelic.instrumentation.azure.cosmos427.ErrorConsumer;
import com.newrelic.instrumentation.azure.cosmos427.Utils;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Weave
public abstract class RxDocumentClientImpl {

	public Flux<FeedResponse<Document>> readAllDocuments(
			String collectionLink,
			PartitionKey partitionKey,
			CosmosQueryRequestOptions options) {
		HashMap<String,Object> attributes = new HashMap<String, Object>();

		Utils.addAttribute(attributes, "Operation", "CreateCollection");
		Utils.addAttribute(attributes, "CollectionLink", collectionLink);
		Utils.addPartitionKey(attributes, partitionKey);


		Flux<FeedResponse<Document>> result = Weaver.callOriginal();
		DatastoreParameters params = DatastoreParameters.product(CosmosUtils.COSMOSDB).collection(CosmosUtils.getIDFromLink(collectionLink)).operation("readAllDocuments").build();
		CompletionRunnable<Subscription> runnable = new CompletionRunnable<Subscription>("RxDocumentClientImpl/readAllDocuments", params, NewRelic.getAgent().getTransaction());
		runnable.setAttributes(attributes);
		ErrorConsumer errorConsumer = new ErrorConsumer(runnable);

		return result.doOnSubscribe(runnable).doOnCancel(runnable).doOnTerminate(runnable).doOnError(errorConsumer);

	}

	public Mono<ResourceResponse<Document>> deleteAllDocumentsByPartitionKey(String collectionLink, PartitionKey partitionKey, RequestOptions options) {
		HashMap<String,Object> attributes = new HashMap<String, Object>();

		Utils.addAttribute(attributes, "Operation", "CreateCollection");
		Utils.addAttribute(attributes, "CollectionLink", collectionLink);
		Utils.addPartitionKey(attributes, partitionKey);
		
		Mono<ResourceResponse<Document>> result = Weaver.callOriginal();
		DatastoreParameters params = DatastoreParameters.product(CosmosUtils.COSMOSDB).collection(CosmosUtils.getIDFromLink(collectionLink)).operation("deleteAllDocumentsByPartitionKey").build();
		CompletionRunnable<Subscription> runnable = new CompletionRunnable<Subscription>("RxDocumentClientImpl/deleteAllDocumentsByPartitionKey", params, NewRelic.getAgent().getTransaction());
		runnable.setAttributes(attributes);
		ErrorConsumer errorConsumer = new ErrorConsumer(runnable);
		
		return result.doOnSubscribe(runnable).doOnTerminate(runnable).doOnCancel(runnable).doOnError(errorConsumer);		
	}

	public Mono<ResourceResponse<Document>> deleteDocument(String documentLink, InternalObjectNode internalObjectNode, RequestOptions options) {
		HashMap<String,Object> attributes = new HashMap<String, Object>();

		Utils.addAttribute(attributes, "Operation", "CreateCollection");
		Utils.addAttribute(attributes, "DocumentLink", documentLink);
		
		Mono<ResourceResponse<Document>> result = Weaver.callOriginal();
		DatastoreParameters params = DatastoreParameters.product(CosmosUtils.COSMOSDB).collection(CosmosUtils.getIDFromLink(documentLink)).operation("deleteDocument").build();
		CompletionRunnable<Subscription> runnable = new CompletionRunnable<Subscription>("RxDocumentClientImpl/deleteDocument", params, NewRelic.getAgent().getTransaction());
		runnable.setAttributes(attributes);
		ErrorConsumer errorConsumer = new ErrorConsumer(runnable);
		
		return result.doOnSubscribe(runnable).doOnTerminate(runnable).doOnCancel(runnable).doOnError(errorConsumer);		
	}
	
	
}
