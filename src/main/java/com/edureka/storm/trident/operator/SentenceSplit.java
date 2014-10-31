package com.edureka.storm.trident.operator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

public class SentenceSplit extends BaseFunction {
	private static final Logger LOG = LoggerFactory.getLogger(SentenceSplit.class);
	
	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		System.out.println(tuple.toString());
		for (String word : tuple.getString(0).toLowerCase().split(" ")) {
			if (word.length() > 0) {
				collector.emit(new Values(word));
			}
		}
	}
}
