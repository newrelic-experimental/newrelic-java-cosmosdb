package com.azure.cosmos;

import java.util.HashMap;

import org.reactivestreams.Subscription;

import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.PartitionKey;
import com.newrelic.api.agent.DatastoreParameters;
import com.newrelic.api.agent.NewRelic;
import com.newrelic.api.agent.weaver.Weave;
import com.newrelic.api.agent.weaver.Weaver;
import com.newrelic.instrumentation.azure.cosmos.CompletionRunnable;
import com.newrelic.instrumentation.azure.cosmos.CosmosUtils;
import com.newrelic.instrumentation.azure.cosmos.Utils;

import reactor.core.publisher.Mono;

@Weave
public abstract class CosmosAsyncContainer {
	
	public abstract String getId();

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
				;
		DatastoreParameters params = DatastoreParameters.product(CosmosUtils.COSMOSDB).collection(collectionName).operation("readItem").build();
		CompletionRunnable<Subscription> runnable = new CompletionRunnable<Subscription>("CosmosAsyncContainer/readItem", params, NewRelic.getAgent().getTransaction());
		runnable.setAttributes(attributes);
		return result.doOnSubscribe(runnable).doOnTerminate(runnable);
	}
}
