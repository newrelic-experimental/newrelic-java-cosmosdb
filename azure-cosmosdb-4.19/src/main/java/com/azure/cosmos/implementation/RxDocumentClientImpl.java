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
import com.newrelic.instrumentation.azure.cosmos419.AzureUtils;
import com.newrelic.instrumentation.azure.cosmos419.CompletionRunnable;
import com.newrelic.instrumentation.azure.cosmos419.CosmosUtils;
import com.newrelic.instrumentation.azure.cosmos419.ErrorConsumer;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Weave
public abstract class RxDocumentClientImpl {

	public Flux<FeedResponse<Document>> readAllDocuments(
			String collectionLink,
			PartitionKey partitionKey,
			CosmosQueryRequestOptions options) {
		HashMap<String,Object> attributes = new HashMap<String, Object>();

		AzureUtils.addAttribute(attributes, "Operation", "readAllDocuments");
		AzureUtils.addAttribute(attributes, "CollectionLink", collectionLink);
		AzureUtils.addPartitionKey(attributes, partitionKey);


		Flux<FeedResponse<Document>> result = Weaver.callOriginal();
		DatastoreParameters params = DatastoreParameters.product(CosmosUtils.COSMOSDB).collection(CosmosUtils.getIDFromLink(collectionLink)).operation("readAllDocuments").build();
		CompletionRunnable<Subscription> runnable = new CompletionRunnable<Subscription>("RxDocumentClientImpl/readAllDocuments", params, NewRelic.getAgent().getTransaction());
		runnable.setAttributes(attributes);
		ErrorConsumer errorConsumer = new ErrorConsumer(runnable);

		return result.doOnSubscribe(runnable).doOnCancel(runnable).doOnTerminate(runnable).doOnError(errorConsumer);

	}

	public Mono<ResourceResponse<Document>> deleteAllDocumentsByPartitionKey(String collectionLink, PartitionKey partitionKey, RequestOptions options) {
		HashMap<String,Object> attributes = new HashMap<String, Object>();

		AzureUtils.addAttribute(attributes, "Operation", "deleteAllDocumentsByPartitionKey");
		AzureUtils.addAttribute(attributes, "CollectionLink", collectionLink);
		AzureUtils.addPartitionKey(attributes, partitionKey);
		
		Mono<ResourceResponse<Document>> result = Weaver.callOriginal();
		DatastoreParameters params = DatastoreParameters.product(CosmosUtils.COSMOSDB).collection(CosmosUtils.getIDFromLink(collectionLink)).operation("deleteAllDocumentsByPartitionKey").build();
		CompletionRunnable<Subscription> runnable = new CompletionRunnable<Subscription>("RxDocumentClientImpl/deleteAllDocumentsByPartitionKey", params, NewRelic.getAgent().getTransaction());
		runnable.setAttributes(attributes);
		ErrorConsumer errorConsumer = new ErrorConsumer(runnable);
		
		return result.doOnSubscribe(runnable).doOnTerminate(runnable).doOnCancel(runnable).doOnError(errorConsumer);		
	}
	
	
}
