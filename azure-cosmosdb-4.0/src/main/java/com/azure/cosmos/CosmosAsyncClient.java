package com.azure.cosmos;

import org.reactivestreams.Subscription;

import com.azure.core.util.Context;
import com.azure.cosmos.models.CosmosDatabaseResponse;
import com.azure.cosmos.models.ThroughputProperties;
import com.newrelic.api.agent.DatastoreParameters;
import com.newrelic.api.agent.NewRelic;
import com.newrelic.api.agent.weaver.Weave;
import com.newrelic.api.agent.weaver.Weaver;
import com.newrelic.instrumentation.azure.cosmos40.CompletionRunnable;
import com.newrelic.instrumentation.azure.cosmos40.CosmosUtils;
import com.newrelic.instrumentation.azure.cosmos40.ErrorConsumer;

import reactor.core.publisher.Mono;

@Weave
public abstract class CosmosAsyncClient {

	 @SuppressWarnings("unused")
	private Mono<CosmosDatabaseResponse> createDatabaseIfNotExistsInternal(CosmosAsyncDatabase database, ThroughputProperties throughputProperties, Context context) {
			Mono<CosmosDatabaseResponse> result = Weaver.callOriginal();
			
			String name = database.getId();
			DatastoreParameters params = DatastoreParameters.product(CosmosUtils.COSMOSDB).collection(name).operation("createContainerIfNotExists").build();
			CompletionRunnable<Subscription> runnable = new CompletionRunnable<Subscription>("CosmosAsyncContainer/createContainerIfNotExists", params, NewRelic.getAgent().getTransaction());
			ErrorConsumer errorConsumer = new ErrorConsumer(runnable);
			
			return result.doOnSubscribe(runnable).doOnCancel(runnable).doOnTerminate(runnable).doOnTerminate(runnable).doOnError(errorConsumer);
	 }

}
