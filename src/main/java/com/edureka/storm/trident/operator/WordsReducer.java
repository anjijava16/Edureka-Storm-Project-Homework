package com.edureka.storm.trident.operator;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.edureka.storm.trident.model.WordNotSeenRecord;

import storm.trident.operation.ReducerAggregator;
import storm.trident.tuple.TridentTuple;


public class WordsReducer 
//extends BaseFunction
implements ReducerAggregator<WordNotSeenRecord> 
{
	private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(WordsReducer.class);
    
    @Override
    public WordNotSeenRecord init() {
        return null;
    }

	@Override
    public WordNotSeenRecord reduce(WordNotSeenRecord curr, TridentTuple tuple) {
		WordNotSeenRecord wordNotSeen = curr;
		if(wordNotSeen==null){
			String tupleVal = (String) tuple.getValueByField("wordtime");
			String[] splitted = tupleVal.split(":");
			wordNotSeen = new WordNotSeenRecord(
					splitted[0],new Long(splitted[1])
			);
		}
		return wordNotSeen;
    }

}
