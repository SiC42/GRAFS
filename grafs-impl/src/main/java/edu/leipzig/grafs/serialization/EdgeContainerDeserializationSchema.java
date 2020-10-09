package edu.leipzig.grafs.serialization;


import edu.leipzig.grafs.model.EdgeContainer;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

public class EdgeContainerDeserializationSchema implements DeserializationSchema<EdgeContainer> {

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

  @Override
  public boolean isEndOfStream(EdgeContainer edgeContainer) {
    return false;
  }

  @Override
  public TypeInformation<EdgeContainer> getProducedType() {
    return TypeInformation.of(EdgeContainer.class);
  }
}
