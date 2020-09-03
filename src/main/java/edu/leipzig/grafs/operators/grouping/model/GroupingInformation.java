package edu.leipzig.grafs.operators.grouping.model;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

public class GroupingInformation implements Serializable {

  private boolean useLabel;
  private Set<String> groupingKeys;

  public GroupingInformation() {
    useLabel = false;
    groupingKeys = new HashSet<>();
  }

  public GroupingInformation(boolean useLabel,
      Set<String> groupingKeys) {
    this.useLabel = useLabel;
    this.groupingKeys = groupingKeys;
  }

  public GroupingInformation(Set<String> groupingKeys) {
    this();
    this.groupingKeys = groupingKeys;
  }

  public boolean addKey(String key) {
    return groupingKeys.add(key);
  }

  public Set<String> getKeys() {
    return groupingKeys;
  }

  public void useLabel(boolean b) {
    useLabel = b;
  }

  public boolean shouldUseLabel() {
    return useLabel;
  }

  @Override
  public String toString() {
    return "GroupingInformation{" +
        "useLabel=" + useLabel +
        ", groupingKeys=" + groupingKeys +
        '}';
  }
}