package com.azure.cosmos;

import java.util.HashMap;

import org.reactivestreams.Subscription;

import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosContainerRequestOptions;
import com.azure.cosmos.models.CosmosContainerResponse;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.PartitionKey;
import com.newrelic.api.agent.DatastoreParameters;
import com.newrelic.api.agent.NewRelic;
import com.newrelic.api.agent.Trace;
import com.newrelic.api.agent.weaver.Weave;
import com.newrelic.api.agent.weaver.Weaver;
import com.newrelic.instrumentation.azure.cosmos40.CompletionRunnable;
import com.newrelic.instrumentation.azure.cosmos40.CosmosUtils;
import com.newrelic.instrumentation.azure.cosmos40.ErrorConsumer;
import com.newrelic.instrumentation.azure.cosmos40.Utils;

import reactor.core.publisher.Mono;

@Weave
public abstract class CosmosAsyncContainer {
	
	public abstract String getId();
	
	abstract String getLink();
	
	public <T> Mono<CosmosItemResponse<T>> createItem(T item, CosmosItemRequestOptions options) {
		Mono<CosmosItemResponse<T>> result = Weaver.callOriginal();
		String collectionName = getId();
		if(collectionName == null || collectionName.isEmpty()) {
			collectionName = item != null ? item.getClass().getSimpleName() : "";
		}
		
		DatastoreParameters params = DatastoreParameters.product(CosmosUtils.COSMOSDB).collection(collectionName).operation("createItem").build();
		CompletionRunnable<Subscription> runnable = new CompletionRunnable<Subscription>("CosmosAsyncContainer/createItem", params, NewRelic.getAgent().getTransaction());
		ErrorConsumer errorConsumer = new ErrorConsumer(runnable);
		return result.doOnSubscribe(runnable).doOnTerminate(runnable).doOnCancel(runnable).doOnError(errorConsumer);
	}
	
	public Mono<CosmosContainerResponse> delete(CosmosContainerRequestOptions options) {
		Mono<CosmosContainerResponse> result = Weaver.callOriginal();
		String containerName = CosmosUtils.getIDFromLink(getLink());
		if(containerName == null || containerName.isEmpty()) {
			containerName = "";
		}
		
		DatastoreParameters params = DatastoreParameters.product(CosmosUtils.COSMOSDB).collection(containerName).operation("delete").build();
		CompletionRunnable<Subscription> runnable = new CompletionRunnable<Subscription>("CosmosAsyncContainer/delete", params, NewRelic.getAgent().getTransaction());
		ErrorConsumer errorConsumer = new ErrorConsumer(runnable);
		return result.doOnSubscribe(runnable).doOnTerminate(runnable).doOnCancel(runnable).doOnError(errorConsumer);
	}
	
	@Trace
	public Mono<CosmosItemResponse<Object>> deleteItem(String itemId, PartitionKey partitionKey, CosmosItemRequestOptions options) {
		HashMap<String, Object> attributes = new HashMap<String, Object>();
		Utils.addPartitionKey(attributes, partitionKey);
		Utils.addAttribute(attributes, "ItemID", itemId);
		Utils.addPartitionKey(attributes, partitionKey);
		
		Mono<CosmosItemResponse<Object>> result = Weaver.callOriginal();
		String collectionName = CosmosUtils.getIDFromLink(getLink());
		if(collectionName == null || collectionName.isEmpty()) {
			collectionName = "";
		}
		
		DatastoreParameters params = DatastoreParameters.product(CosmosUtils.COSMOSDB).collection(collectionName).operation("deleteItem").build();
		CompletionRunnable<Subscription> runnable = new CompletionRunnable<Subscription>("CosmosAsyncContainer/deleteItem", params, NewRelic.getAgent().getTransaction());
		runnable.setAttributes(attributes);
		ErrorConsumer errorConsumer = new ErrorConsumer(runnable);
		return result.doOnSubscribe(runnable).doOnTerminate(runnable).doOnCancel(runnable).doOnError(errorConsumer);
	}
	
	public Mono<CosmosContainerResponse> read(CosmosContainerRequestOptions options) {
		Mono<CosmosContainerResponse> result = Weaver.callOriginal();
		String containerName = CosmosUtils.getIDFromLink(getLink());
		if(containerName == null || containerName.isEmpty()) {
			containerName = "";
		}
	
		DatastoreParameters params = DatastoreParameters.product(CosmosUtils.COSMOSDB).collection(containerName).operation("read").build();
		CompletionRunnable<Subscription> runnable = new CompletionRunnable<Subscription>("CosmosAsyncContainer/read", params, NewRelic.getAgent().getTransaction());
		ErrorConsumer errorConsumer = new ErrorConsumer(runnable);
		return result.doOnSubscribe(runnable).doOnTerminate(runnable).doOnCancel(runnable).doOnError(errorConsumer);
	}
	
	
	
	public <T> Mono<CosmosItemResponse<T>> readItem(String itemId, PartitionKey partitionKey,CosmosItemRequestOptions options, Class<T> itemType) {
		HashMap<String, Object> attributes = new HashMap<String, Object>();
		Utils.addPartitionKey(attributes, partitionKey);
		Utils.addAttribute(attributes, "ItemID", itemId);
		Utils.addAttribute(attributes, "ItemType", itemType.getSimpleName());
		
		Mono<CosmosItemResponse<T>> result = Weaver.callOriginal();
		String collectionName = getId();
		if(collectionName == null || collectionName.isEmpty()) {
			collectionName = itemType != null ? itemType.getSimpleName() : "";
		}
		
		DatastoreParameters params = DatastoreParameters.product(CosmosUtils.COSMOSDB).collection(collectionName).operation("readItem").build();
		CompletionRunnable<Subscription> runnable = new CompletionRunnable<Subscription>("CosmosAsyncContainer/readItem", params, NewRelic.getAgent().getTransaction());
		runnable.setAttributes(attributes);
		ErrorConsumer errorConsumer = new ErrorConsumer(runnable);
		return result.doOnSubscribe(runnable).doOnTerminate(runnable).doOnCancel(runnable).doOnError(errorConsumer);
	}
	
	public Mono<CosmosContainerResponse> replace(CosmosContainerProperties containerProperties, CosmosContainerRequestOptions options) {
		Mono<CosmosContainerResponse> result = Weaver.callOriginal();
		String containerName = CosmosUtils.getIDFromLink(getLink());
		if(containerName == null || containerName.isEmpty()) {
			containerName = "";
		}
		
		DatastoreParameters params = DatastoreParameters.product(CosmosUtils.COSMOSDB).collection(containerName).operation("replace").build();
		CompletionRunnable<Subscription> runnable = new CompletionRunnable<Subscription>("CosmosAsyncContainer/replace", params, NewRelic.getAgent().getTransaction());
		ErrorConsumer errorConsumer = new ErrorConsumer(runnable);
		return result.doOnSubscribe(runnable).doOnTerminate(runnable).doOnCancel(runnable).doOnError(errorConsumer);
	}
	
	public <T> Mono<CosmosItemResponse<T>> replaceItem(T item, String itemId, PartitionKey partitionKey, CosmosItemRequestOptions options) {
		HashMap<String, Object> attributes = new HashMap<String, Object>();
		Utils.addPartitionKey(attributes, partitionKey);
		Utils.addAttribute(attributes, "ItemID", itemId);
		Utils.addAttribute(attributes, "ItemType", item.getClass().getSimpleName());
		
		Mono<CosmosItemResponse<T>> result = Weaver.callOriginal();
		String collectionName = getId();
		if(collectionName == null || collectionName.isEmpty()) {
			collectionName = item != null ? item.getClass().getSimpleName() : "";
		}
		
		DatastoreParameters params = DatastoreParameters.product(CosmosUtils.COSMOSDB).collection(collectionName).operation("replaceItem").build();
		CompletionRunnable<Subscription> runnable = new CompletionRunnable<Subscription>("CosmosAsyncContainer/replaceItem", params, NewRelic.getAgent().getTransaction());
		runnable.setAttributes(attributes);
		ErrorConsumer errorConsumer = new ErrorConsumer(runnable);
		return result.doOnSubscribe(runnable).doOnTerminate(runnable).doOnCancel(runnable).doOnError(errorConsumer);
	}
	
	public <T> Mono<CosmosItemResponse<T>> upsertItem(T item, CosmosItemRequestOptions options) {
		HashMap<String, Object> attributes = new HashMap<String, Object>();
		Utils.addAttribute(attributes, "ItemType", item.getClass().getSimpleName());
		
		Mono<CosmosItemResponse<T>> result = Weaver.callOriginal();
		String collectionName = getId();
		if(collectionName == null || collectionName.isEmpty()) {
			collectionName = item != null ? item.getClass().getSimpleName() : "";
		}
		
		DatastoreParameters params = DatastoreParameters.product(CosmosUtils.COSMOSDB).collection(collectionName).operation("upsertItem").build();
		CompletionRunnable<Subscription> runnable = new CompletionRunnable<Subscription>("CosmosAsyncContainer/upsertItem", params, NewRelic.getAgent().getTransaction());
		runnable.setAttributes(attributes);
		ErrorConsumer errorConsumer = new ErrorConsumer(runnable);
		return result.doOnSubscribe(runnable).doOnTerminate(runnable).doOnCancel(runnable).doOnError(errorConsumer);
	}
}
