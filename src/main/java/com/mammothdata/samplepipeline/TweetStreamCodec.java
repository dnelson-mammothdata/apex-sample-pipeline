package com.mammothdata.samplepipeline;

import java.io.Serializable;
import com.datatorrent.api.StreamCodec;
import com.datatorrent.lib.codec.JavaSerializationStreamCodec;
import com.mammothdata.samplepipeline.Tweet;

public class TweetStreamCodec extends JavaSerializationStreamCodec<Tweet> implements Serializable {
    @Override
    public int getPartition(Tweet o) {
      // This isn't something I would use in production ;)
      return o.getUser_name().hashCode() ^ o.getCreated_at().hashCode();
  }  
}
