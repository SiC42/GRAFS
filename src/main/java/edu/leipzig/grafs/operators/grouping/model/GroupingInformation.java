package edu.leipzig.grafs.operators.grouping.model;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class GroupingInformation implements Serializable {

  public static final String LABEL_SYMBOL = ":label";
  private final Set<String> groupingKeys;
  private boolean useLabel;

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
    for (var key : groupingKeys) {
      addKey(key);
    }
  }

  public boolean addKey(String key) {
    if (key.equals(LABEL_SYMBOL)) {
      useLabel(true);
      return true;
    } else {
      return groupingKeys.add(key);
    }
  }

  public boolean addKeys(Collection<String> keys) {
    boolean modified = false;
    for (var key : keys) {
      if (addKey(key)) {
        modified = true;
      }
    }
    return modified;
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