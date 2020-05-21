package streaming.operators.grouping.model;

import java.io.Serializable;
import java.util.HashSet;

public class GroupingInformation implements Serializable {

  public boolean useLabel;
  public boolean useMembership;
  public HashSet<String> groupingKeys;

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
}