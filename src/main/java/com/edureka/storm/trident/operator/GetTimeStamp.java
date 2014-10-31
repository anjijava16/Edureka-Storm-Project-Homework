package com.edureka.storm.trident.operator;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.List;

public class GetTimeStamp extends BaseFunction {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(GetTimeStamp.class);

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
    	String word = (String) tuple.getValue(0);
    	Long time = System.currentTimeMillis();
    	
    	String key = word+":"+time.toString();
    	
        List<Object> values = new ArrayList<Object>();
        values.add(key);
        values.add(time);
        collector.emit(values);
    }
}
