package edu.leipzig.grafs.setup.serialization;

import java.nio.charset.Charset;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

public class SimpleStringSchemaWithEnd extends SimpleStringSchema {
  public static final String END_OF_STREAM_LABEL = "_endOfStream";

  public SimpleStringSchemaWithEnd() {
  }

  public SimpleStringSchemaWithEnd(Charset charset) {
    super(charset);
  }

  public boolean isEndOfStream(String nextElement) {
    return nextElement.equals(END_OF_STREAM_LABEL);
  }


}
