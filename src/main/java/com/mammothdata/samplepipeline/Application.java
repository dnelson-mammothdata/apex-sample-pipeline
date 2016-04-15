package com.mammothdata.samplepipeline;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.kafka.KafkaSinglePortStringInputOperator;
import com.datatorrent.lib.io.IdempotentStorageManager;
import com.datatorrent.lib.io.fs.AbstractFileInputOperator;
import com.datatorrent.lib.stream.StreamMerger;
import org.apache.hadoop.conf.Configuration;

@ApplicationAnnotation(name="ExactlyOnceExampleApplication")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {

    // This sets our window to 1 second in length
    dag.getAttributes().put(DAG.STREAMING_WINDOW_SIZE_MILLIS, 1000);

    KafkaSinglePortStringInputOperator kafkaInput = dag.addOperator("kafkaInput", new KafkaSinglePortStringInputOperator());
    kafkaInput.setIdempotentStorageManager(new IdempotentStorageManager.FSIdempotentStorageManager());
    System.out.println("kafkaPArser");
    TweetJsonParser kafkaParser = dag.addOperator("kafkaparser", new TweetJsonParser());
    System.out.println("setClaszz");
    kafkaParser.setClazz(Tweet.class);
    System.out.println("kafkaParser metadata");
    dag.getMeta(kafkaParser).getMeta(kafkaParser.out).getAttributes().put(Context.PortContext.TUPLE_CLASS, Tweet.class);

    AbstractFileInputOperator.FileLineInputOperator hdfsInput = dag.addOperator("hdfsInput", new AbstractFileInputOperator.FileLineInputOperator());
//    hdfsInput.setDirectory("foo");
    TweetJsonParser hdfsParser = dag.addOperator("hdfsParser", new TweetJsonParser());
    hdfsParser.setClazz(Tweet.class);
    dag.getMeta(hdfsParser).getMeta(hdfsParser.out).getAttributes().put(Context.PortContext.TUPLE_CLASS, Tweet.class);

    StreamMerger mergeOperator = dag.addOperator("mergeOperator", new StreamMerger<Tweet>());
    // This should hold the tuples at a multiple of 120 * STREAMING_WINDOW_SIZE_MILLIS, i.e. two minutes
    dag.getOperatorMeta("mergeOperator").getAttributes().put(Context.OperatorContext.APPLICATION_WINDOW_COUNT, 120);

    TweetAggregateOperator tweetAggregator = dag.addOperator("TweetAggregateOperator", new TweetAggregateOperator());
    dag.setInputPortAttribute(tweetAggregator.input, Context.PortContext.STREAM_CODEC, new TweetStreamCodec());

    AggregationWriterOperator userWriter = dag.addOperator("HDFSUserWriter", new AggregationWriterOperator());
    userWriter.setFileName("user");
    userWriter.setFilePath("wibble");
    dag.getOperatorMeta("HDFSUserWriter").getAttributes().put(Context.OperatorContext.APPLICATION_WINDOW_COUNT, 3600);
    AggregationWriterOperator sourceWriter = dag.addOperator("HDFSSourceWriter", new AggregationWriterOperator());
    sourceWriter.setFileName("source");
    sourceWriter.setFilePath("wibble");
    dag.getOperatorMeta("HDFSSourceWriter").getAttributes().put(Context.OperatorContext.APPLICATION_WINDOW_COUNT, 3600);

    dag.addStream("TweetsFromKafka", kafkaInput.outputPort, kafkaParser.in);
    dag.addStream("TweetsFromHDFS", hdfsInput.output, hdfsParser.in);

    dag.addStream("MergeFromKafka", kafkaParser.out, mergeOperator.data1);
    dag.addStream("MergeFromHDFS", hdfsParser.out, mergeOperator.data2);
    dag.addStream("Aggregation", mergeOperator.out, tweetAggregator.input);
    dag.addStream("WriteUserInformation", tweetAggregator.userOutput, userWriter.input);
    dag.addStream("WriteSourceInformaiton", tweetAggregator.sourceOutput, sourceWriter.input);

  }
}
