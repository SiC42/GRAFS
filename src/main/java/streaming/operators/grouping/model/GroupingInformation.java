package streaming.operators.grouping.model;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

public class GroupingInformation implements Serializable {

  private boolean useLabel;
  private boolean useMembership;
  private HashSet<String> groupingKeys;

  public GroupingInformation() {
    useLabel = false;
    useMembership = false;
    groupingKeys = new HashSet<>();
  }

  public GroupingInformation(boolean useLabel, boolean useMembership,
      HashSet<String> groupingKeys) {
    this.useLabel = useLabel;
    this.useMembership = useMembership;
    this.groupingKeys = groupingKeys;
  }

  public GroupingInformation(HashSet<String> groupingKeys) {
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
    useLabel = true;
  }

  public boolean shouldUseLabel() {
    return useLabel;
  }

  public boolean shouldUseMembership() {
    // TODO: implement it or discuss ignoring it
    return false;
  }
}