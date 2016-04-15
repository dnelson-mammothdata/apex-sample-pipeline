package com.mammothdata.samplepipeline;

import com.datatorrent.lib.parser.Parser;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * Created by dnelson on 4/14/16.
 */
public class TweetJsonParser extends Parser {

  public void processTuple(String inputTuple){
    Object tuple = convert(inputTuple);
    if (tuple == null && err.isConnected()) {
      errorTupleCount++;
      err.emit(processErorrTuple(inputTuple));
      return;
    }
    if (out.isConnected()) {
      out.emit(createTweet(inputTuple));
    }
  }

  public Tweet createTweet(String tweetJson) {
    Tweet tweet = new Tweet();

    JsonElement inputJson = new JsonParser().parse(tweetJson); //from gson
    JsonObject inputRoot = inputJson.getAsJsonObject();
    tweet.setId(inputRoot.get("id").getAsLong());
    tweet.setSource(inputRoot.get("source").toString());
    tweet.setCreated_at(inputRoot.get("created_at").toString());

    JsonObject inputUser = inputRoot.get("user").getAsJsonObject();
    tweet.setUser_id(inputUser.get("id").getAsLong());
    tweet.setUser_name(inputUser.get("name").toString());

    return tweet;
  }

  @Override
  public Tweet convert(Object tuple) {
    return createTweet((String) tuple);
  }

  @Override
  public Object processErorrTuple(Object o) {
    errorTupleCount++;
    return null;
  }
}
