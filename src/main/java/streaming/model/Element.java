package streaming.model;

import java.io.Serializable;
import java.util.Objects;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.Property;
import org.gradoop.common.model.impl.properties.PropertyValue;

public abstract class Element implements Serializable {

  private final GradoopId id;
  private Properties properties;
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


  public String getLabel() {
    return label;
  }

  public void setLabel(String label) {
    this.label = label;
  }


  public PropertyValue getPropertyValue(String key) {
    return (this.properties != null) ? this.properties.get(key) : null;
  }

  public void setProperty(String key, PropertyValue value) {
    initProperties();
    properties.set(key, value);
  }

  public void setProperty(String key, Object value) {
    initProperties();
    properties.set(key, value);
  }

  public void setProperty(Property property) {
    initProperties();
    properties.set(property);
  }

  public boolean hasProperty(String key) {
    return this.properties != null && this.properties.containsKey(key);
  }

  public Properties getProperties() {
    return properties;
  }

  public Iterable<String> getPropertyKeys() {
    return (properties != null) ? properties.getKeys() : null;
  }

  public int getPropertyCount() {
    return (this.properties != null) ? this.properties.size() : 0;
  }


  @Override
  public String toString() {
    return String.format("%s%s%s{%s}",
        id,
        label == null || label.equals("") ? "" : ":",
        label,
        properties == null ? "" : properties);
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

    return Objects.equals(id, that.id);
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

  /**
   * Initializes the internal properties field if necessary.
   */
  private void initProperties() {
    if (this.properties == null) {
      this.properties = Properties.create();
    }
  }
}
