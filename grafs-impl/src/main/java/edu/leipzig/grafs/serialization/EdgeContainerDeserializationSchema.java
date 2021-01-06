package edu.leipzig.grafs.serialization;


import edu.leipzig.grafs.model.EdgeContainer;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 * Allows Deserialization of Edge Containers in Kafka Sources.
 */
public class EdgeContainerDeserializationSchema implements DeserializationSchema<EdgeContainer> {

  /**
   * Acts as the indicator for Flink to end the stream, as all elements have been processed.
   */
  public static final String END_OF_STREAM_LABEL = "_endOfStream";

  /**
   * Deserialzes an array of bytes into an edge container and returns it.
   *
   * @param bytes bytes representation of an edge container object
   * @return object representation of given bytes
   */
  @Override
  public EdgeContainer deserialize(byte[] bytes) {
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

  /**
   * Returns <tt>true</tt> if a end of stream element is found and thus ends the stream.
   * <p>
   * The edge in the edge container needs to have the END_OF_STREAM_LABEL. * @param edgeContainer
   * element which should be checked if it is an end of stream element
   *
   * @return <tt>true</tt> if the given element contains the END_OF_STREAM_LABEL, <tt>false</tt>
   * otherwise
   */
  @Override
  public boolean isEndOfStream(EdgeContainer edgeContainer) {
    var edge = edgeContainer.getEdge();
    return edgeContainer.getEdge().getLabel().equals(END_OF_STREAM_LABEL);
  }

  /**
   * Returns type information of the elements produced by this class.
   *
   * @return type information of the elements produced by this class
   */
  @Override
  public TypeInformation<EdgeContainer> getProducedType() {
    return TypeInformation.of(EdgeContainer.class);
  }
}
