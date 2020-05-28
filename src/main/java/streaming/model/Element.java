package streaming.model;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;

public class Element implements Serializable {

  private final GradoopId id;
  private String label;
  private Map<String, String> properties;

  public Element() {
    this(GradoopId.get(), "", new HashMap<>());
  }


  public Element(GradoopId id, String label, Map<String, String> properties) {
    this.id = id;
    this.label = label;
    this.properties = new HashMap<>();
    this.properties.putAll(properties);
  }


  public Element(Element otherElement) {
    this.id = otherElement.getId();
    this.label = otherElement.getLabel();
    this.properties = otherElement.getProperties();
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


  @Override
  public String toString() {
    return String.format("%s properties=%s memberships=%s",
        label, properties);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Element edge = (Element) o;
    return Objects.equals(properties, edge.properties) &&
        Objects.equals(label, edge.label);
  }

  public GradoopId getId(){
    return id;
  }

  @Override
  public int hashCode() {
    return Objects.hash(properties, label);
  }
}
