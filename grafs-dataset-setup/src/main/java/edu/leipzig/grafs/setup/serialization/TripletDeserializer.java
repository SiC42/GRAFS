package edu.leipzig.grafs.setup.serialization;


import edu.leipzig.grafs.model.Triplet;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import org.apache.kafka.common.serialization.Deserializer;

public class TripletDeserializer implements Deserializer<Triplet> {

  @Override
  public Triplet deserialize(String s, byte[] bytes) {
    try {
      var bais = new ByteArrayInputStream(bytes);
      var ois = new ObjectInputStream(bais);
      var triplet = (Triplet) ois.readObject();
      ois.close();
      return triplet;
    } catch (IOException | ClassNotFoundException e) {
      e.printStackTrace();
      return null;
    }
  }
}
