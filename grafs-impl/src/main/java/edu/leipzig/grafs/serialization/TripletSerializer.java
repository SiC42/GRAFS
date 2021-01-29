package edu.leipzig.grafs.serialization;


import edu.leipzig.grafs.model.Triplet;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Serializer used to serialize triplets into a byte array.
 */
public class TripletSerializer implements Serializer<Triplet> {


  /**
   * Serializes the given triplet into a byte array and returns it
   *
   * @param s       ignored
   * @param triplet triplet which should be serialized into byte representation
   * @return byte representation of given triplet
   */
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
