package streaming.model;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class GraphElementInformation {

  private String label;
  private Map<String, String> properties;
  private Set<Long> memberships;

  public GraphElementInformation() {
    label = "";
    this.properties = new HashMap<>();
    this.memberships = new HashSet<>();
  }


  public GraphElementInformation(String label, Map<String, String> properties,
      Set<Long> memberships) {
    this.label = label;
    this.properties = properties;
    this.memberships = memberships;
  }

  public GraphElementInformation(String fromStr, String toStr, String contentStr) {
    this();
    properties.put("id", contentStr);
    memberships.add(0L);
  }

  public String getLabel() {
    return label;
  }

  public void setLabel(String label) {
    this.label = label;
  }


  public String getProperty(String key) {
    return properties.get(key);
  }

  public void addProperty(String key, String value) {
    properties.put(key, value);
  }

  public boolean containsProperty(String key) {
    return properties.containsKey(key);
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  public Set<Long> getMemberships() {
    return memberships;
  }

  public void addMembership(Long membership) {
    this.memberships.add(membership);
  }

  public void removeMembership(String membership) {
    this.memberships.remove(membership);
  }


  @Override
  public String toString() {
    return String.format("%s properties=%s memberships=%s",
        label, properties, memberships);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    GraphElementInformation edge = (GraphElementInformation) o;
    return Objects.equals(properties, edge.properties) &&
        Objects.equals(memberships, edge.memberships) &&
        Objects.equals(label, edge.label);
  }

  @Override
  public int hashCode() {
    return Objects.hash(properties, memberships, label);
  }
}
