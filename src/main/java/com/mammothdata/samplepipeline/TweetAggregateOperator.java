package com.mammothdata.samplepipeline;


import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;

import java.util.HashMap;
import java.util.Map;

public class TweetAggregateOperator extends BaseOperator {

  // Note that this implementation will blow up memory-wise over time, 
  // as each key is id+day.

  HashMap<String, Integer> values = new HashMap<String, Integer>();
  HashMap<String, Integer> sources = new HashMap<String, Integer>();

  public final transient DefaultInputPort<Tweet> input = new DefaultInputPort<Tweet>() {
    @Override
    public void process(Tweet tuple) {
      String userKey = tuple.getId() + "-" + tuple.getDay();
      String sourceKey = tuple.getSource();

      if (values.containsKey(userKey)) {
        Integer count = values.remove(userKey);
        values.put(userKey, count+1);
      } else {
        values.put(userKey, 1);
      }

      if (sources.containsKey(sourceKey)) {
        Integer count = sources.remove(sourceKey);
        sources.put(sourceKey, count+1);
      } else {
        sources.put(sourceKey, 1);
      }
    }
  };

  public final transient DefaultOutputPort<Map<String,Integer>> userOutput = new DefaultOutputPort<Map<String,Integer>>();
  public final transient DefaultOutputPort<Map<String,Integer>> sourceOutput = new DefaultOutputPort<Map<String,Integer>>();

  @Override
  public void endWindow() {
    userOutput.emit(values);
    sourceOutput.emit(sources);
  }  
}