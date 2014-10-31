package com.edureka.storm.trident.topology;

import java.io.IOException;
import java.util.List;

import org.apache.commons.io.IOUtils;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;

import com.edureka.storm.trident.operator.GetTimeStamp;
import com.edureka.storm.trident.operator.WordsReducer;
import com.edureka.storm.trident.operator.SentenceSplit;
import com.edureka.storm.trident.sprout.LogReadEventSpout;
import com.edureka.storm.trident.operator.WordNotMatchFilter;
import com.google.common.collect.Lists;
import com.edureka.storm.trident.state.LogNotSeenFactory;

import storm.trident.Stream;
import storm.trident.TridentTopology;

public class WordNotSeenTopology {
	
	private static LogReadEventSpout sprout = new LogReadEventSpout();
	private static List<String> words = Lists.newArrayList();
	
	
	@SuppressWarnings("unchecked")
	private static void readWords(){
        String[] sentences;
        try {
	        sentences = (String[]) IOUtils.readLines(
	        		ClassLoader.getSystemClassLoader()
	        		.getResourceAsStream("storm_destination_file.txt")).toArray(new String[0]);
	        for(int i=0;i<sentences.length;i++){
	        	String[] splitted = sentences[i].split(" ");
	        	for(int j=0;j<splitted.length;j++){
	        		if(splitted[j].trim()!="")
	        			words.add(splitted[j].toLowerCase());
	        	}
	        }
        } catch (IOException e) {
	        // TODO Auto-generated catch block
	        e.printStackTrace();
        }
	}

    public static StormTopology buildTopology() {
        	TridentTopology topology = new TridentTopology();        
        	Stream inputStream = topology.newStream("sentencesprout", sprout);
        	readWords(); // filter out words that are avail in the storm_destination_file
        	
        	inputStream
        	    .each(new Fields("sentence"), new SentenceSplit(), new Fields("word"))
        	    .each(new Fields("word"), new WordNotMatchFilter(words))
        	    .each(new Fields("word"),new GetTimeStamp(),new Fields("wordtime"))
        	    .groupBy(new Fields("wordtime"))
        	    .persistentAggregate(new LogNotSeenFactory(), new Fields("wordtime")
        	    	, new WordsReducer(),new Fields("logrecord"))
        	    ;
//        	    .newValuesStream()
//        	    .each(new Fields("logrecord"), new Debug());
        	
        	
        	return topology.build();
    }

    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("wns", conf, buildTopology());
        Thread.sleep(200000);
        cluster.shutdown();
    }
}
