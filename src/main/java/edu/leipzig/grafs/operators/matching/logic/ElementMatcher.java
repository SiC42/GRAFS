package edu.leipzig.grafs.operators.matching.logic;

import edu.leipzig.grafs.model.GraphElement;
import org.gradoop.common.util.GradoopConstants;

public class ElementMatcher {

  static boolean matchesQueryElem(GraphElement queryElem, GraphElement elem) {
    if (!labelMatches(queryElem.getLabel(), elem.getLabel())) {
      return false;
    }
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
    return true;
  }

  private static boolean labelMatches(String queryLabel, String label) {
    return queryLabel.equals(label)
        || queryLabel.equals(GradoopConstants.DEFAULT_VERTEX_LABEL)
        || label.equals(GradoopConstants.DEFAULT_VERTEX_LABEL);
  }

}
