package streaming.model.grouping;

import java.io.Serializable;
import java.util.HashSet;

public class ElementGroupingInformation implements Serializable {
    public boolean shouldGroupByLabel;
    public boolean shouldGroupByMembership;
    public HashSet<String> groupingKeys;

    public ElementGroupingInformation(){
        shouldGroupByLabel = false;
        shouldGroupByMembership = false;
        groupingKeys = new HashSet<>();
    }

    public ElementGroupingInformation(boolean shouldGroupByLabel, boolean shouldGroupByMembership, HashSet<String> groupingKeys) {
        this.shouldGroupByLabel = shouldGroupByLabel;
        this.shouldGroupByMembership = shouldGroupByMembership;
        this.groupingKeys = groupingKeys;
    }

    public ElementGroupingInformation(HashSet<String> groupingKeys) {
        this();
        this.groupingKeys = groupingKeys;
    }
}