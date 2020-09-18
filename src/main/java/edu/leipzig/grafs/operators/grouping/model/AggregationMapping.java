package edu.leipzig.grafs.operators.grouping.model;

import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.gradoop.common.model.impl.id.GradoopIdSet;

public class AggregationMapping implements Serializable {

  private Map<String, PropertiesAggregationFunction> propertyMappingMap;
  private SerializableBiFunction<GradoopIdSet, GradoopIdSet, GradoopIdSet> membershipAggregation;

  public AggregationMapping() {
    propertyMappingMap = new HashMap<>();
    membershipAggregation = null;
  }

  public void addAggregationForProperty(String key,
      final PropertiesAggregationFunction accumulator) {
    propertyMappingMap.put(key, accumulator);
  }

  public PropertiesAggregationFunction getAggregationForProperty(String key) {
    return propertyMappingMap.get(key);
  }

  public boolean containsAggregationForProperty(String key) {
    return propertyMappingMap.containsKey(key);
  }

  public Optional<SerializableBiFunction<GradoopIdSet, GradoopIdSet, GradoopIdSet>> getAggregationForMembership() {
    return membershipAggregation != null ? Optional.of(membershipAggregation) : Optional.empty();
  }

  public void setAggregationForMembership(
      SerializableBiFunction<GradoopIdSet, GradoopIdSet, GradoopIdSet> aggregation) {
    membershipAggregation = aggregation;
  }

  public Set<AggregationMappingEntry> entrySet() {
    Set<AggregationMappingEntry> aggregationMappingEntrySet = new HashSet<>();
    for (var e : propertyMappingMap.entrySet()) {
      var aggEntry = new AggregationMappingEntry(e.getKey(), e.getValue());
      aggregationMappingEntrySet.add(aggEntry);
    }
    return aggregationMappingEntrySet;
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
    membershipAggregation = (SerializableBiFunction<GradoopIdSet, GradoopIdSet, GradoopIdSet>) in
        .readObject();

  }

  private void readObjectNoData()
      throws ObjectStreamException {

  }

  public void addAggregationMappingEntry(AggregationMappingEntry mappingEntry) {
    addAggregationForProperty(mappingEntry.getPropertyKey(), mappingEntry.getAggregationFunction());
  }
}
