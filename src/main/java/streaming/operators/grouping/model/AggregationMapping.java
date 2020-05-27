package streaming.operators.grouping.model;

import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import streaming.operators.grouping.functions.PropertiesAggregationFunction;

public class AggregationMapping implements Serializable {

  public HashMap<String, PropertiesAggregationFunction> aggregationMap;

  public AggregationMapping() {
    aggregationMap = new HashMap<>();
  }

  public void addAggregation(String key, final PropertiesAggregationFunction accumulator) {
    PropertiesAggregationFunction aF = accumulator;
    aggregationMap.put(key, aF);
  }

  public PropertiesAggregationFunction get(String key) {
    return aggregationMap.get(key);
  }

  public Set<Map.Entry<String, PropertiesAggregationFunction>> entrySet() {
    return aggregationMap.entrySet();
  }

  public boolean contains(String key) {
    return aggregationMap.containsKey(key);
  }


  private void writeObject(java.io.ObjectOutputStream out)
      throws IOException {
    out.writeInt(aggregationMap.size());
    for (Map.Entry<String, PropertiesAggregationFunction> entry : aggregationMap.entrySet()) {
      out.writeObject(entry.getKey());
      out.writeObject(entry.getValue());
    }
  }

  private void readObject(java.io.ObjectInputStream in)
      throws IOException, ClassNotFoundException {
    this.aggregationMap = new HashMap<>();
    int size = in.readInt();
    for (int i = 0; i < size; i++) {
      String key = (String) in.readObject();
      PropertiesAggregationFunction aggFun = (PropertiesAggregationFunction) in.readObject();
      aggregationMap.put(key, aggFun);
    }

  }

  private void readObjectNoData()
      throws ObjectStreamException {

  }
}
