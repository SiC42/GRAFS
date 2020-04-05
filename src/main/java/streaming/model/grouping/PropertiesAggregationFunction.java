package streaming.model.grouping;

import java.io.IOException;
import java.io.ObjectStreamException;

public class PropertiesAggregationFunction implements
    SerializableBiFunction<String, String, String> {

  private String identity;
  private SerializableBiFunction<String, String, String> function;

  public PropertiesAggregationFunction(String identity,
      SerializableBiFunction<String, String, String> function) {
    this.identity = identity;
    this.function = function;
  }

  public String getIdentity() {
    return identity;
  }

  public String apply(String pV1, String pV2) {
    return function.apply(pV1, pV2);
  }

  private void writeObject(java.io.ObjectOutputStream out)
      throws IOException {
    out.writeObject(identity);
    out.writeObject(function);
  }

  private void readObject(java.io.ObjectInputStream in)
      throws IOException, ClassNotFoundException {
    this.identity = (String) in.readObject();
    this.function = (SerializableBiFunction<String, String, String>) in.readObject();

  }

  private void readObjectNoData()
      throws ObjectStreamException {

  }

}
