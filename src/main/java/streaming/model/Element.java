package streaming.model;

import java.io.Serializable;
import java.util.Objects;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.Property;
import org.gradoop.common.model.impl.properties.PropertyValue;

public class Element implements Serializable {

  private final GradoopId id;
  private final Properties properties;
  private String label;

  public Element() {
    this(GradoopId.get(), "", Properties.create());
  }

  public Element(GradoopId id) {
    this(id, "", Properties.create());
  }


  public Element(GradoopId id, String label, Properties properties) {
    this.id = id;
    this.label = label;
    this.properties = properties;
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


  public PropertyValue getPropertyValue(String key) {
    return properties.get(key);
  }

  public void setProperty(String key, PropertyValue value) {
    properties.set(key, value);
  }

  public void setProperty(String key, Object value) {
    properties.set(key, value);
  }

  public void setProperty(Property property) {
    properties.set(property);
  }

  public boolean hasProperty(String key) {
    return properties.containsKey(key);
  }

  public Properties getProperties() {
    return properties;
  }

  public Iterable<String> getPropertyKeys() {
    return properties.getKeys();
  }

  public int getPropertyCount() {
    return (this.properties != null) ? this.properties.size() : 0;
  }


  @Override
  public String toString() {
    return String.format("%s properties=%s",
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
    Element that = (Element) o;
    return this.getId() == that.getId();
  }

  public GradoopId getId() {
    return id;
  }

  @Override
  public int hashCode() {
    return Objects.hash(properties, label);
  }

  public PropertyValue removeProperty(String key) {
    return this.properties != null ? properties.remove(key) : null;
  }
}
