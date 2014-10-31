package com.edureka.storm.trident.operator;

import java.util.List;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

public class WordNotMatchFilter extends BaseFilter {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(WordNotMatchFilter.class);

    private static List<String>words;
    public WordNotMatchFilter(List<String> words){
    	this.words = words;
    }
    
    @Override
    public boolean isKeep(TridentTuple tuple) {
    	
        String word = (String) tuple.getValue(0);
        if(words.contains(word)){
        	return false;
        }else{
        	return true; //only keep if new word
        }
        
//        Integer code = Integer.parseInt(diagnosis.diagnosisCode);
//        if (code.intValue() <= 322) {
//            LOG.debug("Emitting disease [" + diagnosis.diagnosisCode + "]");
//            return true;
//        } else {
//            LOG.debug("Filtering disease [" + diagnosis.diagnosisCode + "]");
//            return false;
//        }
        
    }
}