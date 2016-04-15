package com.mammothdata.samplepipeline;

import org.junit.Assert;
import org.junit.Before;
import org.junit.After;

import org.junit.Ignore;
import org.junit.Test;
import java.util.Map;
import java.util.HashMap;
import com.datatorrent.lib.codec.JavaSerializationStreamCodec;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.io.fs.FilterStreamCodec;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.api.Attribute;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import java.io.FileFilter;

import java.io.File;

public class AggregationWriterOperatorTest {

  @Test
  public void testByte() {
    HashMap<String,Integer> t = new HashMap<String,Integer>();
    t.put("1", 2);
    t.put("3", 4);

    AggregationWriterOperator operator = new AggregationWriterOperator();
    operator.setFileName("test");    
    operator.setFilePath("test");    
    Attribute.AttributeMap.DefaultAttributeMap attributeMap = new Attribute.AttributeMap.DefaultAttributeMap();
    OperatorContextTestHelper.TestIdOperatorContext testOperatorContext = testOperatorContext = new OperatorContextTestHelper.TestIdOperatorContext(0, attributeMap);

    operator.setup(testOperatorContext);
    operator.input.process(t);

    File dir = new File("test");
    FileFilter fileFilter = new WildcardFileFilter("*");

    Assert.assertEquals("Checking for local file creation",1,dir.listFiles(fileFilter).length);

    operator.teardown();
  }

  @After
  public void tearDown(){
     try {
      FileUtils.deleteDirectory(new File("test"));
    }
    catch(Exception e) {}
  }

}

