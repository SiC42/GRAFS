package streaming.model.grouping;

import java.io.Serializable;
import java.util.HashSet;

public class ElementGroupingInformation implements Serializable {

  public boolean useLabel;
  public boolean useMembership;
  public HashSet<String> groupingKeys;

  public ElementGroupingInformation() {
    useLabel = false;
    useMembership = false;
    groupingKeys = new HashSet<>();
  }

  public ElementGroupingInformation(boolean useLabel, boolean useMembership,
      HashSet<String> groupingKeys) {
    this.useLabel = useLabel;
    this.useMembership = useMembership;
    this.groupingKeys = groupingKeys;
  }

  public ElementGroupingInformation(HashSet<String> groupingKeys) {
    this();
    this.groupingKeys = groupingKeys;
  }
}