package edu.leipzig.grafs.setup.reader;

import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.serialization.TripletDeserializationSchema;
import java.io.Closeable;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

public class SerializedTripletFileReader implements Closeable {

  private final ObjectInputStream ois;
  private Triplet nextTriplet;

  private boolean hasNext;

  public SerializedTripletFileReader(String fileStr)
      throws IOException, ClassNotFoundException {
    var fileOutputStream = new FileInputStream(fileStr);
    this.ois = new ObjectInputStream(fileOutputStream);
    nextTriplet = (Triplet) ois.readObject();
    hasNext = true;
  }

  private boolean isLast(Triplet triplet) {
    return triplet.getEdge().getLabel()
        .equals(TripletDeserializationSchema.END_OF_STREAM_LABEL);
  }

  public Triplet getNext() throws IOException, ClassNotFoundException {
    var lastTriplet = nextTriplet;
    if (isLast(lastTriplet)) {
      hasNext = false;
    } else {
      nextTriplet = (Triplet) ois.readObject();
    }
    return lastTriplet;
  }

  public boolean hasNext() {
    return hasNext;
  }


  @Override
  public void close() throws IOException {
    ois.close();
  }
}
