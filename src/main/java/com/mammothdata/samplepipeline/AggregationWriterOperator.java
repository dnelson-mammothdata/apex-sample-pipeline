package com.mammothdata.samplepipeline;

import com.datatorrent.lib.io.fs.AbstractFileOutputOperator;

import java.io.UnsupportedEncodingException;
import java.util.Map;

public class AggregationWriterOperator extends AbstractFileOutputOperator<Map<String, Integer>>
{
  
  private static final String charsetName = "UTF-8";
  private static final String nl = System.lineSeparator();

  private String fileName; 
  private transient final StringBuilder sb = new StringBuilder();
  private String base;

  @Override
  public void endWindow() {
    if (null != fileName) {
      requestFinalize(fileName);
    }
    super.endWindow();
  }

  @Override
  protected String getFileName(Map<String, Integer> tuple) {
    return fileName + this.currentWindow;
  }

  protected void setFileName(String _fileName) {
    fileName = _fileName;
  }

  @Override
  public void setFilePath(String _filePath) {
    filePath = _filePath;
  }



  @Override
  protected byte[] getBytesForTuple(Map<String, Integer> tuple) {
    for (Map.Entry<String, Integer> x : tuple.entrySet()) {
      sb.append(x.getKey());
      sb.append(":");
      sb.append(x.getValue());
      sb.append(nl);

    }
    try {
      final byte[] result = sb.toString().getBytes(charsetName);
      return result;
    } 
    catch (UnsupportedEncodingException e) {
      throw new RuntimeException("Should never get here", e);
    }

  }
}