package com.edureka.storm.trident.state;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.edureka.storm.trident.model.WordNotSeenRecord;

import storm.trident.state.map.IBackingMap;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LogNotSeenBackingMap implements IBackingMap<WordNotSeenRecord> {
    private static final Logger LOG = LoggerFactory.getLogger(LogNotSeenBackingMap.class);
    Map<String, WordNotSeenRecord> storage = new ConcurrentHashMap<String, WordNotSeenRecord>();

    @Override
    public List<WordNotSeenRecord> multiGet(List<List<Object>> keys) {
        List<WordNotSeenRecord> values = new ArrayList<WordNotSeenRecord>();
        for (List<Object> key : keys) {
        	WordNotSeenRecord value = storage.get(key.get(0));
            if (value != null) {
                values.add(value);
            }else{
            	String[] splitted= ((String)key.get(0)).split(":");
            	values.add(new WordNotSeenRecord((String)splitted[0],new Long(splitted[1])));
            }
        }
        return values;
    }

    @Override
    public void multiPut(List<List<Object>> keys, List<WordNotSeenRecord> vals) {
        for (int i = 0; i < keys.size(); i++) {
        	String key = (String) keys.get(i).get(0);
        	String[] splitted = key.split(":");
        	WordNotSeenRecord rec = vals.get(i);
            LOG.info("Persisting [" + splitted[0] + "] ==> [" + rec.toString() + "]");
            storage.put((String)splitted[0],rec);            
//            System.out.println(storage.toString());
            
            try(PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter("storm_log.txt", true)))) {
                out.println("New Record: --> "+rec.getWord()+"\t\t"+"Has arrived at "+new Date(rec.getTime()));
            }catch (IOException e) {
                //exception handling left as an exercise for the reader
            }
            
        }
    }
}
