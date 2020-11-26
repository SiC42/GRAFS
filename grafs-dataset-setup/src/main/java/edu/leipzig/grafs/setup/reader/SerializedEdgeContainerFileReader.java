package edu.leipzig.grafs.setup.reader;

import edu.leipzig.grafs.model.EdgeContainer;
import edu.leipzig.grafs.serialization.EdgeContainerDeserializationSchema;
import java.io.Closeable;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

public class SerializedEdgeContainerFileReader implements Closeable {

  private final ObjectInputStream ois;
  private EdgeContainer nextEc;

  private boolean hasNext;

  public SerializedEdgeContainerFileReader(String fileStr)
      throws IOException, ClassNotFoundException {
    var fileOutputStream = new FileInputStream(fileStr);
    this.ois = new ObjectInputStream(fileOutputStream);
    nextEc = (EdgeContainer) ois.readObject();
    hasNext = true;
  }

  private boolean isLast(EdgeContainer ec) {
    return ec.getEdge().getLabel()
        .equals(EdgeContainerDeserializationSchema.END_OF_STREAM_LABEL);
  }

  public EdgeContainer getNext() throws IOException, ClassNotFoundException {
    var lastEc = nextEc;
    if (isLast(lastEc)) {
      hasNext = false;
    } else {
      nextEc = (EdgeContainer) ois.readObject();
    }
    return lastEc;
  }

  public boolean hasNext() {
    return hasNext;
  }


  @Override
  public void close() throws IOException {
    ois.close();
  }
}
