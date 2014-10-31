package com.edureka.storm.trident.sprout;

import storm.trident.operation.TridentCollector;
import storm.trident.spout.ITridentSpout.Emitter;
import storm.trident.topology.TransactionAttempt;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Lists;


public class LogReadEventEmitter implements Emitter<Long>, Serializable {
    
	private static final long serialVersionUID = 1L;
    AtomicInteger successfulTransactions = new AtomicInteger(0);
    
    private BufferedReader br;
    public LogReadEventEmitter() {
    	br = newBufferedReader();
    }
    private BufferedReader newBufferedReader(){
    	return new BufferedReader(
    			new InputStreamReader(
    					ClassLoader.getSystemClassLoader().getResourceAsStream("words.txt")));
    }
    
    
    @Override
    public void emitBatch(TransactionAttempt tx, Long coordinatorMeta
    		, TridentCollector collector) {
    	String line;
    	try {
    		for(int i=0;i<8;i++){
    			line = br.readLine();
    			if(line==null){
    				br = newBufferedReader();
    				line = br.readLine();
    			}
    			collector.emit(Lists.newArrayList((Object)line));
    		}
	        
        } catch (IOException e) {
	        // TODO Auto-generated catch block
	        e.printStackTrace();
        } 
    }

    @Override
    public void success(TransactionAttempt tx) {
        successfulTransactions.incrementAndGet();
    }

    @Override
    public void close() {
    }

}
