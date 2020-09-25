package edu.leipzig.grafs.serialization;


import edu.leipzig.grafs.model.EdgeContainer;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import org.apache.kafka.common.serialization.Serializer;

public class EdgeContainerSerializer implements Serializer<EdgeContainer> {


  @Override
  public byte[] serialize(String s, EdgeContainer ec) {
    try {
      var baos = new ByteArrayOutputStream();
      var oos = new ObjectOutputStream(baos);
      oos.writeObject(ec);
      oos.close();
      return baos.toByteArray();
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }

}
