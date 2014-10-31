package com.edureka.storm.trident.state;

import backtype.storm.task.IMetricsContext;
import storm.trident.state.State;
import storm.trident.state.StateFactory;

import java.util.Map;

@SuppressWarnings("rawtypes")
public class LogNotSeenFactory implements StateFactory {
    private static final long serialVersionUID = 1L;

    @Override
    public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
        return new LogNotSeenState(new LogNotSeenBackingMap());
    }
}
