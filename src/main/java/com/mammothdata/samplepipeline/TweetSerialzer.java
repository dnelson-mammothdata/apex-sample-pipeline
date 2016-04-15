package com.mammothdata.samplepipeline;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

/**
 * Created by dnelson on 4/13/16.
 */
public class TweetSerialzer<INPUT> extends BaseOperator {


  public transient DefaultInputPort<INPUT> in = new DefaultInputPort<INPUT>()
  {
    @Override
    public void process(INPUT inputTuple)
    {
      processTuple(inputTuple);
    }
  };

  public transient DefaultOutputPort<String> out = new DefaultOutputPort<String>();

  private void processTuple(Object tuple) {
    Tweet tweet = new Tweet();

    try {
      tweet = (Tweet) tuple;
    }
    catch(ClassCastException e) {
      System.out.println("Not a tweet! " + e.getMessage());
    }

    String serializedTweet = "Tweet:";
    serializedTweet += "\tid:\t" + tweet.getId().toString() + "\n";
    serializedTweet += "\tuser_name:\t" + tweet.getUser_name() + "\n";
    serializedTweet += "\tuser,id:\t" + tweet.getId().toString() + "\n";
//    serializedTweet += "\tfrom_user:\t" + tweet.getFrom_user().toString() + "\n";
//    serializedTweet += "\tfrom_user_id:\t" + tweet.getUser_id() + "\n";

    if (out.isConnected()) {
      out.emit(serializedTweet);
    }
  }


//  public String convert(INPUT tuple) {
//    Tweet tweet = new Tweet();
//
//    try {
//      tweet = (Tweet) tuple;
//    }
//    catch(ClassCastException e) {
//      System.out.println("Not a tweet! " + e.getMessage());
//    }
//
//    String serializedTweet = "Tweet:";
//    serializedTweet += "\tid:\t" + tweet.getId().toString() + "\n";
//    serializedTweet += "\tuser_name:\t" + tweet.getUser_name() + "\n";
//    serializedTweet += "\tfrom_user:\t" + tweet.getFrom_user().toString() + "\n";
//    serializedTweet += "\tfrom_user_id:\t" + tweet.getUser_id() + "\n";
//
//    System.out.println(serializedTweet);
//
//    return serializedTweet;
//  }
}
