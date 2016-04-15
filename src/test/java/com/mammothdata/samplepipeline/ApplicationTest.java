/**
 * Put your copyright and license info here.
 */
package com.mammothdata.samplepipeline;

import com.datatorrent.api.LocalMode;
import com.datatorrent.contrib.kafka.KafkaOperatorTestBase;
import com.datatorrent.contrib.kafka.KafkaTestProducer;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.ConstraintViolationException;
import java.io.File;
import java.util.List;

/**
 * Test the application in local mode.
 */
public class ApplicationTest
{
  private final KafkaOperatorTestBase kafkaLauncher = new KafkaOperatorTestBase();
  private static final Logger LOG = LoggerFactory.getLogger(ApplicationTest.class);
  private static final String TOPIC = "apex-sample-pipeline-test";
  private MiniDFSCluster hdfsCluster;
  private Configuration conf = new Configuration();

  @Before
  public void beforeTest() throws Exception {
    kafkaLauncher.baseDir = "target/kafka/" + this.getClass().getName();
    FileUtils.deleteDirectory(new File(kafkaLauncher.baseDir));
    kafkaLauncher.startZookeeper();
    kafkaLauncher.startKafkaServer();
    kafkaLauncher.createTopic(0, TOPIC);

//    File baseDir = new File("target/hdfs/" + this.getClass().getName());
//    FileUtil.fullyDelete(baseDir);
//    System.out.println("MiniDFSCluster.HDFS_MINIDFS_BASEDIR: " + MiniDFSCluster.HDFS_MINIDFS_BASEDIR);
//    System.out.println("baseDir.getPath(): " + baseDir.getPath());
//    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getPath());
//    MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
//    hdfsCluster = builder.build();
  }

  @After
  public void afterTest() {
    kafkaLauncher.stopKafkaServer();
    kafkaLauncher.stopZookeeper();
  }

  @Test
  public void testApplication() throws Exception {
    try {
      // Pull in sample data and shovel it into kafka
      KafkaTestProducer p = new KafkaTestProducer(TOPIC);
      List<String> lines = IOUtils.readLines(
              this.getClass().getResourceAsStream("/data/sample.short.json"),
              "UTF-8"
      );
      p.setMessages(lines);
      LOG.info("Kafka data loaded");

      new Thread(p).start();

      LocalMode lma = LocalMode.newInstance();
      Configuration conf = new Configuration(false);
      conf.addResource(this.getClass().getResourceAsStream("/META-INF/properties.xml"));
      conf.set("dt.operator.kafkaInput.prop.topic", TOPIC);
      conf.set("dt.operator.kafkaInput.prop.zookeeper", "localhost:" + KafkaOperatorTestBase.TEST_ZOOKEEPER_PORT[0]);
      conf.set("dt.operator.kafkaInput.prop.maxTuplesPerWindow", "1"); // consume one word per window

      lma.prepareDAG(new Application(), conf);
      LocalMode.Controller lc = lma.getController();

      LOG.info("Application run started");
      lc.run(10000);
      LOG.info("Application run finished");

      lc.shutdown();

    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
  }

}
