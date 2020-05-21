package streaming.model;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class GraphElementInformation {

  private String label;
  private Map<String, String> properties;
  private long membership;

  public GraphElementInformation() {
    label = "";
    this.properties = new HashMap<>();
    this.membership = -1;
  }


  public GraphElementInformation(String label, Map<String, String> properties,
      long membership) {
    this.label = label;
    this.properties = properties;
    this.membership = membership;
  }

  public GraphElementInformation(String fromStr, String toStr, String contentStr) {
    this();
    properties.put("id", contentStr);
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

  public long getMembership() {
    return membership;
  }

  public void setMembership(long membership) {
    this.membership = membership;
  }


  @Override
  public String toString() {
    return String.format("%s properties=%s membership=%d",
        label, properties, membership);
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
        Objects.equals(membership, edge.membership) &&
        Objects.equals(label, edge.label);
  }

  @Override
  public int hashCode() {
    return Objects.hash(properties, membership, label);
  }
}
