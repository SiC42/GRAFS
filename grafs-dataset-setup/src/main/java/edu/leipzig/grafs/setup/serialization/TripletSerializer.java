package edu.leipzig.grafs.setup.serialization;

import edu.leipzig.grafs.model.Triplet;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import org.apache.kafka.common.serialization.Serializer;

public class TripletSerializer implements
    Serializer<Triplet> {

  @Override
  public byte[] serialize(String s, Triplet triplet) {
    try {
      var baos = new ByteArrayOutputStream();
      var oos = new ObjectOutputStream(baos);
      oos.writeObject(triplet);
      oos.close();
      return baos.toByteArray();
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }
}
