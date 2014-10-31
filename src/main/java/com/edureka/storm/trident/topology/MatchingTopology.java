package com.edureka.storm.trident.topology;

import java.io.IOException;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.storm.hdfs.trident.format.FileNameFormat;
import org.apache.storm.hdfs.trident.format.RecordFormat;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.edureka.storm.trident.operator.WordCountCount;
import com.edureka.storm.trident.operator.SentenceSplit;
import com.edureka.storm.trident.sprout.LogReadEventSpout;
import com.edureka.storm.trident.operator.WordNotMatchFilter;
import com.google.common.collect.Lists;

import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Debug;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.MemoryMapState;

public class MatchingTopology {
	
	private static LogReadEventSpout sprout = new LogReadEventSpout();
	private static List<String> words = Lists.newArrayList();
	
	
	@SuppressWarnings("unchecked")
	private static void readWords(){
        String[] sentences;
        try {
	        sentences = (String[]) IOUtils.readLines(ClassLoader.getSystemClassLoader().getResourceAsStream("storm_destination_file.txt")).toArray(new String[0]);
	        for(int i=0;i<sentences.length;i++){
	        	String[] splitted = sentences[i].split(" ");
	        	for(int j=0;j<splitted.length;j++){
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
        	readWords();
        	
        	inputStream
        	    .each(new Fields("sentence"), new SentenceSplit(), new Fields("word"))
        	    .each(new Fields("word"), new WordNotMatchFilter(words))
//        	    .groupBy(new Fields("word"))
        	    .persistentAggregate(new MemoryMapState.Factory(), new WordCountCount(), new Fields("count")).newValuesStream()
        	    .each(new Fields("word","count"), new Debug());
        	
        	
//        	 FileNameFormat fileNameFormat = new DefaultFileNameFormat()
//             .withPrefix("trident")
//             .withExtension(".txt")
//             .withPath("/trident");
//        	 
//        	 RecordFormat recordFormat = new DelimitedRecordFormat()
//             .withFields(hdfsFields);
//HdfsState.Options options = new HdfsState.HdfsFileOptions()
//            .withFileNameFormat(fileNameFormat)
//            .withRecordFormat(recordFormat)
//            .withRotationPolicy(rotationPolicy)
//            .withFsUrl("hdfs://localhost:54310");
//
//StateFactory factory = new HdfsStateFactory().withOptions(options);
//
//TridentState state = stream
//            .partitionPersist(factory, hdfsFields, new HdfsUpdater(), new Fields());
        	
        	
//        	.each(new Fields("occupancyEvent"), new ExtractCorrelationId(), new Fields("correlationId"))
//        	.groupBy(new Fields("correlationId"))
//        	.persistentAggregate( PeriodBackingMap.FACTORY, new Fields("occupancyEvent")
//        						, new PeriodBuilder(), new Fields("presencePeriod"))
//        	.newValuesStream()
        	
        	return topology.build();
    }

    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("cdc", conf, buildTopology());
        Thread.sleep(200000);
        cluster.shutdown();
    }
}
