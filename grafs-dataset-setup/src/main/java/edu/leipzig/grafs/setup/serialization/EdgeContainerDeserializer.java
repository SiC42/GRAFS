package edu.leipzig.grafs.setup.serialization;


import edu.leipzig.grafs.model.EdgeContainer;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import org.apache.kafka.common.serialization.Deserializer;

public class EdgeContainerDeserializer implements Deserializer<EdgeContainer> {

  @Override
  public EdgeContainer deserialize(String s, byte[] bytes) {
    try {
      var bais = new ByteArrayInputStream(bytes);
      var ois = new ObjectInputStream(bais);
      var ec = (EdgeContainer) ois.readObject();
      ois.close();
      return ec;
    } catch (IOException | ClassNotFoundException e) {
      e.printStackTrace();
      return null;
    }
  }
}
