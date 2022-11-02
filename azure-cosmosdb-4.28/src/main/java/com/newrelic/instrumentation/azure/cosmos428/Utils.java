package com.newrelic.instrumentation.azure.cosmos428;

import java.util.Map;

import com.azure.cosmos.implementation.Database;
import com.azure.cosmos.implementation.Document;
import com.azure.cosmos.implementation.DocumentCollection;
import com.azure.cosmos.implementation.Offer;
import com.azure.cosmos.implementation.StoredProcedure;
import com.azure.cosmos.implementation.User;
import com.azure.cosmos.models.PartitionKey;
import com.newrelic.agent.database.SqlObfuscator;
import com.newrelic.api.agent.QueryConverter;

public class Utils {
	
	public static QueryConverter<String> getConverter() {
		return 	new QueryConverter<String>() {
			
			@Override
			public String toRawQueryString(String rawQuery) {
				return rawQuery;
			}
			
			@Override
			public String toObfuscatedQueryString(String rawQuery) {
				SqlObfuscator obfuscator = SqlObfuscator.getDefaultSqlObfuscator();
				
				return obfuscator.obfuscateSql(rawQuery);
			}
		};

	}
	


	public static void addAttribute(Map<String, Object> attributes, String key, Object value) {
		if(attributes != null && key != null && !key.isEmpty() && value != null) {
			attributes.put(key, value);
		}
	}
	
	public static void addPartitionKey(Map<String, Object> attributes, PartitionKey partitionKey) {
		if(partitionKey != null) {
			addAttribute(attributes, "PartitionKey", partitionKey.toString());
		}
	}
	
	public static void addDocumentCollection(Map<String, Object> attributes, DocumentCollection collection) {
		if(collection != null) {
			addAttribute(attributes, "DocumentCollection-DocumentsLink", collection.getDocumentsLink());
			addAttribute(attributes, "DocumentCollection-Id", collection.getId());
			addAttribute(attributes, "DocumentCollection-ResouceId", collection.getResourceId());
		}
	}
	
	public static void addDatabase(Map<String, Object> attributes, Database database) {
		if(database != null) {
			addAttribute(attributes, "Database-CollectionsLink", database.getCollectionsLink());
			addAttribute(attributes, "Database-Id", database.getId());
			addAttribute(attributes, "Database-ResourceId", database.getResourceId());
			
		}
	}
	
	public static void addStoredProcedure(Map<String,Object> attributes, StoredProcedure stored) {
		if(stored != null) {
			addAttribute(attributes, "StoredProcedure-EntityTag", stored.getETag());
			addAttribute(attributes, "StoredProcedure-Id", stored.getId());
			addAttribute(attributes, "StoredProcedure-ResourceId", stored.getResourceId());
		}
	}
	
	public static void addUser(Map<String,Object> attributes, User user) {
		if(user != null) {
			addAttribute(attributes, "User-Id", user.getId());
			addAttribute(attributes, "User-EntityTag", user.getETag());
			addAttribute(attributes, "User-ResourceId", user.getResourceId());
		}
	}
	
	public static void addDocument(Map<String,Object> attributes, Document doc) {
		if(doc != null) {
			addAttribute(attributes, "Document-Id", doc.getId());
			addAttribute(attributes, "Document-EntityTag", doc.getETag());
			addAttribute(attributes, "Document-ResourceId", doc.getResourceId());			
		}
	}
	
	public static void addOffer(Map<String,Object> attributes, Offer offer) {
		if(offer != null) {
			addAttribute(attributes, "Offer-Id", offer.getId());
			addAttribute(attributes, "Offer-OfferType", offer.getOfferType());
			addAttribute(attributes, "Offer-OfferVersion", offer.getOfferVersion());
			addAttribute(attributes, "Offer-Id", offer.getId());
			addAttribute(attributes, "Offer-ResourceId", offer.getResourceId());
		}
	}
}
