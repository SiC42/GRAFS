package edu.leipzig.grafs.operators.matching.logic;

import edu.leipzig.grafs.model.GraphElement;
import org.gradoop.common.util.GradoopConstants;

/**
 * Matches two Elements based on their properties and label
 */
public class ElementMatcher {


  /**
   * Returns <tt>true</tt> if the labels of the given elements match and the properties of the query
   * element are a subset of the properties of the element
   *
   * @param queryElem query element for which a match should be tested
   * @param elem      element which should be tested against a query element
   * @return <tt>true</tt> if the labels of the given elements match and the properties of the query
   * element are a subset of the properties of the element
   */
  static boolean matchesQueryElem(GraphElement queryElem, GraphElement elem) {
    return matchesQueryElem(queryElem, elem, false);
  }

  /**
   * Returns <tt>true</tt> if the labels of the given elements match and the properties of the query
   * element are a subset of the properties of the element
   *
   * @param queryElem query element for which a match should be tested
   * @param elem      element which should be tested against a query element
   * @return <tt>true</tt> if the labels of the given elements match and the properties of the query
   * element are a subset of the properties of the element
   */
  static boolean matchesQueryElem(GraphElement queryElem, GraphElement elem, boolean exact) {
    if (!labelMatches(queryElem.getLabel(), elem.getLabel())) {
      return false;
    }
    if (exact && queryElem.getPropertyCount() != elem.getPropertyCount()) {
      return false;
    }
    if (queryElem.getPropertyKeys() != null) {
      for (var key : queryElem.getPropertyKeys()) {
        if (!elem.hasProperty(key)) {
          return false;
        }
        var queryValue = queryElem.getPropertyValue(key);
        var value = elem.getPropertyValue(key);
        if (!queryValue.equals(value)) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Returns <tt>true</tt> if the given labels match (or the query label is the default label).
   *
   * @param queryLabel query label for which a label should be tested against
   * @param label      label which should be tested for equality against a query label
   * @return <tt>true</tt> if the given labels match (or the query label is the default label)
   */
  private static boolean labelMatches(String queryLabel, String label) {
    return queryLabel.equals(label)
        || queryLabel.equals(GradoopConstants.DEFAULT_VERTEX_LABEL)
        || queryLabel.equals(GradoopConstants.DEFAULT_EDGE_LABEL);
  }

}
