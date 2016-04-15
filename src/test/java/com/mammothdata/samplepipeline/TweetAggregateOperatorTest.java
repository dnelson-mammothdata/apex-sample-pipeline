package com.mammothdata.samplepipeline;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import java.util.Map;

import com.datatorrent.lib.testbench.CollectorTestSink;

public class TweetAggregateOperatorTest {

  private Tweet testTweet;

  @Before
  public void setupTweet() {
    testTweet = new Tweet();
    testTweet.setId(1L);
    testTweet.setCreated_at("Fri Apr 08 14:08:34 +0000 2016");
    testTweet.setSource("wibble");
  }

  @Test
  public void testUserCounts() throws Exception {
    TweetAggregateOperator operator = new TweetAggregateOperator();
    CollectorTestSink userSink = new CollectorTestSink();
    //CollectorTestSink userSink = new CollectorTestSink();
    operator.userOutput.setSink(userSink);

    Integer numtuples = 5;
    for (int i = 0; i < numtuples; i++) {
      operator.input.process(testTweet);
    }
    
    operator.endWindow();
    Assert.assertEquals("Collected tuples", 1, userSink.collectedTuples.size());
    Map<String,Integer> map = (Map<String,Integer>) userSink.collectedTuples.get(0);
    Integer entries =  map.get(testTweet.getId()+"-"+testTweet.getDay());
    Assert.assertEquals("Counted user id / day aggregation", numtuples, entries);
  }

  @Test
  public void testSourceCounts() throws Exception {
    TweetAggregateOperator operator = new TweetAggregateOperator();
    CollectorTestSink sourceSink = new CollectorTestSink();
    //CollectorTestSink userSink = new CollectorTestSink();
    operator.sourceOutput.setSink(sourceSink);

    Integer numtuples = 5;
    for (int i = 0; i < numtuples; i++) {
      operator.input.process(testTweet);
    }
    
    operator.endWindow();
    Assert.assertEquals("Collected tuples", 1, sourceSink.collectedTuples.size());
    Map<String,Integer> map = (Map<String,Integer>) sourceSink.collectedTuples.get(0);
    Integer entries =  map.get(testTweet.getSource());
    Assert.assertEquals("Counted sources", numtuples, entries);
  }
}