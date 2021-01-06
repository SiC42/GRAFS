package edu.leipzig.grafs.serialization;


import edu.leipzig.grafs.model.EdgeContainer;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Serializer used to serialize edge container into a byte array.
 */
public class EdgeContainerSerializer implements Serializer<EdgeContainer> {


  /**
   * Serializes the given edge container into a byte array and returns it
   *
   * @param s  ignored
   * @param ec edge container which should be serialized into byte representation
   * @return byte representation of given edge container
   */
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
