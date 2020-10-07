package edu.leipzig.grafs.model;

import java.io.Serializable;
import java.util.Objects;
import javax.annotation.Nullable;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.Property;
import org.gradoop.common.model.impl.properties.PropertyValue;

/**
 * Data model represents the base element of a graph. While not part of the property graph model,
 * this abstraction eases the modeling of other elements, namely {@link Edge} & {@link Vertex}.
 */
public abstract class Element implements Serializable {

  private final GradoopId id;
  private Properties properties;
  private String label;

  /**
   * Constructs an element with new ID.
   */
  public Element() {
    this(GradoopId.get(), "", null);
  }

  /**
   * Constructs an element with the given ID.
   *
   * @param id ID of the element
   */
  public Element(GradoopId id) {
    this(id, "", null);
  }


  /**
   * Constructs an element with the given information.
   *
   * @param id         ID of the element
   * @param label      label of the element
   * @param properties properties of the element
   */
  public Element(GradoopId id, String label, Properties properties) {
    this.id = id;
    this.label = label;
    this.properties = properties;
  }

  /**
   * Returns the ID of this element.
   *
   * @return the ID of this element
   */
  public GradoopId getId() {
    return id;
  }

  /**
   * Returns the label of this element.
   *
   * @return the label of this element
   */
  public String getLabel() {
    return label;
  }

  /**
   * Sets the label of this element.
   *
   * @param label label to which this elements label should be set to
   */
  public void setLabel(String label) {
    this.label = label;
  }

  /**
   * Returns the value to which the specified key is mapped, or <tt>null</tt> if the properties
   * contain no mapping for the key.
   *
   * @param key the key whose associated property value is to be returned
   * @return the value to which the specified key is mapped, or <tt>null</tt> if the properties
   * contain no mapping for the key
   */
  public PropertyValue getPropertyValue(String key) {
    return (this.properties != null) ? this.properties.get(key) : null;
  }

  /**
   * Associates the specified property value with the specified key. If the map previously contained
   * a mapping for the key, the old value is replaced.
   *
   * @param key   key with which the specified property value is to be associated
   * @param value property value to be associated with the specified key
   */
  public void setProperty(String key, PropertyValue value) {
    initProperties();
    properties.set(key, value);
  }

  /**
   * Associates the specified property value with the specified key. If the map previously contained
   * a mapping for the key, the old value is replaced.
   *
   * @param key   key with which the specified property value is to be associated
   * @param value property value to be associated with the specified key
   */
  public void setProperty(String key, Object value) {
    initProperties();
    properties.set(key, value);
  }

  /**
   * Adds the given property to the properties of this element. If a property with the same property
   * key already exists, it will be replaced by the given property.
   *
   * @param property property to be added to the properties of this element
   */
  public void setProperty(Property property) {
    initProperties();
    properties.set(property);
  }

  /**
   * Returns true if the properties of this element contain a value for the given key
   *
   * @param key key to be check for if there is a associated property
   * @return true if the properties of this element contain a property for the given key
   */
  public boolean hasProperty(String key) {
    return this.properties != null && this.properties.containsKey(key);
  }

  /**
   * Returns the properties of this element.
   *
   * @return properties of this element
   */
  @Nullable
  public Properties getProperties() {
    return properties;
  }

  /**
   * Returns all keys of the properties of this element.
   *
   * @return all keys of the properties of this element
   */
  @Nullable
  public Iterable<String> getPropertyKeys() {
    return (properties != null) ? properties.getKeys() : null;
  }

  /**
   * Returns the amount of properties in this element.
   *
   * @return the amount of properties in this element
   */
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

  /**
   * Returns <tt>true</tt>> if the given object is an <tt>Element</tt> and the IDs match with this
   * element.
   *
   * @param o object to be tested for equality
   * @return <tt>true</tt> if the other object is an <tt>Element</tt> and the IDs match.
   */
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

  @Override
  public int hashCode() {
    return id.hashCode();
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
