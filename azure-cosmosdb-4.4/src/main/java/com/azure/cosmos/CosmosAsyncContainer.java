package com.azure.cosmos;

import org.reactivestreams.Subscription;

import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosItemResponse;
import com.newrelic.api.agent.DatastoreParameters;
import com.newrelic.api.agent.NewRelic;
import com.newrelic.api.agent.weaver.Weave;
import com.newrelic.api.agent.weaver.Weaver;
import com.newrelic.instrumentation.azure.cosmos44.CompletionRunnable;
import com.newrelic.instrumentation.azure.cosmos44.CosmosUtils;
import com.newrelic.instrumentation.azure.cosmos44.ErrorConsumer;

import reactor.core.publisher.Mono;

@Weave
public abstract class CosmosAsyncContainer {
	
	public abstract String getId();
	
	abstract String getLink();
	
	public <T> Mono<CosmosItemResponse<Object>> deleteItem(T item, CosmosItemRequestOptions options) {
		
		Mono<CosmosItemResponse<Object>> result = Weaver.callOriginal();
		String collectionName = CosmosUtils.getIDFromLink(getLink());
		if(collectionName == null || collectionName.isEmpty()) {
			collectionName = "";
		}
		
		DatastoreParameters params = DatastoreParameters.product(CosmosUtils.COSMOSDB).collection(collectionName).operation("deleteItem").build();
		CompletionRunnable<Subscription> runnable = new CompletionRunnable<Subscription>("CosmosAsyncContainer/deleteItem", params, NewRelic.getAgent().getTransaction());
		ErrorConsumer errorConsumer = new ErrorConsumer(runnable);
		return result.doOnSubscribe(runnable).doOnTerminate(runnable).doOnCancel(runnable).doOnError(errorConsumer);
	}

}
