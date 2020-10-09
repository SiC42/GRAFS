package edu.leipzig.grafs.operators.grouping.model;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Data model that contains all the information used to calculate the group of an element.
 */
public class GroupingInformation implements Serializable {

  public static final String LABEL_SYMBOL = ":label";
  private final Set<String> groupingKeys;
  private boolean useLabel;

  /**
   * Constructs the grouping information where nothing is used for grouping.
   */
  public GroupingInformation() {
    useLabel = false;
    groupingKeys = new HashSet<>();
  }

  /**
   * Constructs the grouping information with the given parameters.
   *
   * @param useLabel     use <tt>true</tt> if the label should be used for determining the elements
   *                     group, else <tt>false</tt>
   * @param groupingKeys set of keys whose associated property value in the elements should be used
   *                     to determine the group
   */
  public GroupingInformation(boolean useLabel,
      Set<String> groupingKeys) {
    this.useLabel = useLabel;
    this.groupingKeys = groupingKeys;
  }

  /**
   * Constructs the grouping information with the given parameters. The label will not be used for
   * the grouping.
   *
   * @param groupingKeys set of keys whose associated property value in the elements should be used
   *                     to determine the group
   */
  public GroupingInformation(Set<String> groupingKeys) {
    this();
    for (var key : groupingKeys) {
      addKey(key);
    }
  }

  /**
   * Adds a key to the grouping information or sets the label to be used if {@value #LABEL_SYMBOL}
   * is used.
   *
   * @param key key to be added to the grouping information
   * @return <tt>true</tt> if the grouping information was not used yet
   */
  public boolean addKey(String key) {
    if (key.equals(LABEL_SYMBOL)) {
      useLabel(true);
      return true;
    } else {
      return groupingKeys.add(key);
    }
  }

  /**
   * Adds the given keys to the grouping information or sets the label to be used if for one key
   * "{@value #LABEL_SYMBOL}" is used.
   *
   * @param keys keys to be added to the grouping information
   * @return <tt>true</tt> if the grouping information was not used yet
   */
  public boolean addKeys(Collection<String> keys) {
    boolean modified = false;
    for (var key : keys) {
      if (addKey(key)) {
        modified = true;
      }
    }
    return modified;
  }

  /**
   * Returns all stored keys, that should be used for the grouping.
   *
   * @return all stored keys, that should be used for the grouping.
   */
  public Set<String> getKeys() {
    return groupingKeys;
  }

  /**
   * Set to <tt>true</tt> if the label to be used for grouping, <tt>false</tt> otherwise.
   *
   * @param useLabel determines if the label should be used for grouping
   */
  public void useLabel(boolean useLabel) {
    this.useLabel = useLabel;
  }

  /**
   * Returns <tt>true</tt> if the label should be used for grouping, <tt>false</tt> otherwise.
   *
   * @return <tt>true</tt> if the label should be used for grouping, <tt>false</tt> otherwise
   */
  public boolean shouldUseLabel() {
    return useLabel;
  }

  @Override
  public String toString() {
    return "GroupingInformation{" +
        "useLabel=" + useLabel +
        ", groupingKeys=" + groupingKeys +
        '}';
  }
}