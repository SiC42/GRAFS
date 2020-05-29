package streaming.operators.grouping.model;

import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import streaming.operators.grouping.functions.PropertiesAggregationFunction;
import streaming.operators.grouping.functions.SerializableBiFunction;

public class AggregationMapping implements Serializable {

  private Map<String, PropertiesAggregationFunction> propertyMappingMap;
  private MembershipAggregation membershipAggregation;

  public AggregationMapping() {
    propertyMappingMap = new HashMap<>();
    membershipAggregation = null;
  }

  public void addAggregationForProperty(String key,
      final PropertiesAggregationFunction accumulator) {
    PropertiesAggregationFunction aF = accumulator;
    propertyMappingMap.put(key, aF);
  }

  public PropertiesAggregationFunction getAggregationForProperty(String key) {
    return propertyMappingMap.get(key);
  }

  public boolean containsAggregationForProperty(String key) {
    return propertyMappingMap.containsKey(key);
  }

  public Optional<MembershipAggregation> getAggregationForMembership() {
    return membershipAggregation != null ? Optional.of(membershipAggregation) : Optional.empty();
  }

  public void setAggregationForMembership(MembershipAggregation aggregation) {
    membershipAggregation = aggregation;
  }

  private void writeObject(java.io.ObjectOutputStream out)
      throws IOException {
    out.writeInt(propertyMappingMap.size());
    for (Map.Entry<String, PropertiesAggregationFunction> entry : propertyMappingMap.entrySet()) {
      out.writeObject(entry.getKey());
      out.writeObject(entry.getValue());
    }
    out.writeObject(membershipAggregation);
  }

  private void readObject(java.io.ObjectInputStream in)
      throws IOException, ClassNotFoundException {
    this.propertyMappingMap = new HashMap<>();
    int size = in.readInt();
    for (int i = 0; i < size; i++) {
      String key = (String) in.readObject();
      PropertiesAggregationFunction aggFun = (PropertiesAggregationFunction) in.readObject();
      propertyMappingMap.put(key, aggFun);
    }
    membershipAggregation = (MembershipAggregation) in.readObject();

  }

  private void readObjectNoData()
      throws ObjectStreamException {

  }

  private interface MembershipAggregation extends
      SerializableBiFunction<GradoopIdSet, GradoopIdSet, GradoopIdSet> {

  }
}
