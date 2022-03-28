package com.newrelic.instrumentation.azure.cosmos;

import java.util.Map;
import java.util.function.Consumer;

import com.newrelic.api.agent.DatastoreParameters;
import com.newrelic.api.agent.Segment;
import com.newrelic.api.agent.Transaction;

public class CompletionRunnable<T> implements Runnable,Consumer<T> {
	
	private Segment segment;
	private DatastoreParameters params;
	private String segmentName;
	private Transaction transaction = null;
	private Map<String, Object> attributes = null;
	
	public void setAttributes(Map<String, Object> attributes) {
		this.attributes = attributes;
	}

	public CompletionRunnable(String seg, DatastoreParameters p, Transaction t) {
		params = p;
		transaction = t;
		segmentName = seg;
	}

	@Override
	public void run() {

		if(segment != null) {
			if(params != null) {
				segment.reportAsExternal(params);
			}
			if(attributes != null) {
				segment.addCustomAttributes(attributes);
			}
			segment.end();
			segment = null;
		}
		
	}
	
	
	@Override
	public void accept(T t) {
		if(segmentName != null && !segmentName.isEmpty()) {
			if(transaction != null) {
				segment = transaction.startSegment(segmentName);
			}
		}
	}

}
