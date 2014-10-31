package com.edureka.storm.trident.state;

import com.edureka.storm.trident.model.WordNotSeenRecord;

import storm.trident.state.map.NonTransactionalMap;

public class LogNotSeenState extends NonTransactionalMap<WordNotSeenRecord> {
    protected LogNotSeenState(LogNotSeenBackingMap outbreakBackingMap) {
        super(outbreakBackingMap);
    }
}
