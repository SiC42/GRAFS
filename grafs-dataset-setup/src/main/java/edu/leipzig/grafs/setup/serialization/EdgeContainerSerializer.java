package edu.leipzig.grafs.setup.serialization;

import edu.leipzig.grafs.model.EdgeContainer;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

public class EdgeContainerSerializer {

  public byte[] serialize(EdgeContainer ec) {
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
