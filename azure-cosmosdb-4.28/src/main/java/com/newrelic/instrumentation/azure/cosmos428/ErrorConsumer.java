package com.newrelic.instrumentation.azure.cosmos428;

import java.util.function.Consumer;

import org.reactivestreams.Subscription;

import com.newrelic.api.agent.NewRelic;

public class ErrorConsumer implements Consumer<Throwable> {
	
	CompletionRunnable<Subscription> runnable = null;
	
	public ErrorConsumer(CompletionRunnable<Subscription> r) {
		runnable = r;
	}

	@Override
	public void accept(Throwable t) {
		NewRelic.noticeError(t);
		if(runnable != null) {
			runnable.run();
		}
	}

}
