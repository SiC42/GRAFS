package edu.leipzig.grafs.operators.matching.logic;

import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.operators.matching.model.QueryEdge;
import edu.leipzig.grafs.operators.matching.model.QueryVertex;
import java.util.Collection;
import java.util.List;
import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.s1ck.gdl.model.comparables.ComparableExpression;
import org.s1ck.gdl.model.comparables.PropertySelector;
import org.s1ck.gdl.model.predicates.Predicate;
import org.s1ck.gdl.model.predicates.expressions.Comparison;

public class FilterCandidates implements FilterFunction<Triplet<QueryVertex, QueryEdge>> {

  private final boolean filterForVertexOnly;
  private final List<QueryVertex> queryVertices;
  private final Collection<Triplet<QueryVertex, QueryEdge>> queryTriples;

  public FilterCandidates(Collection<Triplet<QueryVertex, QueryEdge>> queryTriples,
      boolean vertexOnly, List<QueryVertex> queryVertices) {
    this.queryTriples = queryTriples;
    this.filterForVertexOnly = vertexOnly;
    this.queryVertices = queryVertices;
  }

  /**
   * The filter function that evaluates the predicate.
   *
   * <p><strong>IMPORTANT:</strong> The system assumes that the function does not
   * modify the elements on which the predicate is applied. Violating this assumption can lead to
   * incorrect results.
   *
   * @param streamObject The value to be filtered.
   * @return True for values that should be retained, false for values to be filtered out.
   * @throws Exception This method may throw exceptions. Throwing an exception will cause the
   *                   operation to fail and may trigger recovery.
   */
  @Override
  public boolean filter(Triplet<QueryVertex, QueryEdge> streamObject) throws Exception {
    if (filterForVertexOnly) {
      return filterVertexOnly(streamObject);
    } else {
      return filterEdgesWithVertices(streamObject);
    }
  }

  private boolean filterEdgesWithVertices(Triplet<QueryVertex, QueryEdge> triplet) {
    boolean exist;
    boolean match = false;
    for (Triplet<QueryVertex, QueryEdge> qt : queryTriples) { //TODO: get by label
      exist = ElementMatcher.matchesQueryElem(qt.getEdge(), triplet.getEdge());
      exist =
          exist && ElementMatcher.matchesQueryElem(qt.getSourceVertex(), triplet.getSourceVertex());
      exist =
          exist && ElementMatcher.matchesQueryElem(qt.getTargetVertex(), triplet.getTargetVertex());
      // !no need to verify remaining elements
      if (exist) {
        triplet.getSourceVertex().addVariable(qt.getSourceVertex().getVariable());
        triplet.getTargetVertex().addVariable(qt.getTargetVertex().getVariable());
        triplet.getEdge().addVariable(qt.getEdge().getVariable());
        if (!match && qt.getSourceVertex().validatePredicate(triplet.getSourceVertex())
            && qt.getTargetVertex().validatePredicate(triplet.getTargetVertex())
            && qt.getEdge().validatePredicate(triplet.getEdge())
            && validateInterVertexPredicate(qt, triplet)) {
          match = true;
        }
      }
    }
    return match;
  }

  private boolean filterVertexOnly(Triplet<QueryVertex, QueryEdge> triplet) {
    for (QueryVertex queryVertex : queryVertices) {
      if (ElementMatcher.matchesQueryElem(queryVertex, triplet.getSourceVertex())) {
        triplet.getSourceVertex().addVariable(queryVertex.getVariable());
        if (queryVertex.hasPredicateSet()) {
          if (queryVertex.validatePredicate(triplet.getSourceVertex())) {
            return true;
          }
        }

      }
      if (ElementMatcher.matchesQueryElem(queryVertex, triplet.getTargetVertex())) {
        triplet.getTargetVertex().addVariable(queryVertex.getVariable());
        if (queryVertex.hasPredicateSet()) {
          if (queryVertex.validatePredicate(triplet.getSourceVertex())) {
            return true;
          }
        }
      }
    }
    return false;
  }

  private boolean validateInterVertexPredicate(Triplet<QueryVertex, QueryEdge> queryTriplet,
      Triplet<QueryVertex, QueryEdge> triplet) {
    boolean result = true;
    for (Predicate p : queryTriplet.getEdge()
        .getPredicates()) { // only comparisons we have here and with values
      if (result) {
        if (p.getClass() == Comparison.class) {
          Comparison comparison = (Comparison) p;
          ComparableExpression[] list = comparison.getComparableExpressions();
          if (list[0].getClass() == PropertySelector.class
              && list[1].getClass() == PropertySelector.class) {
            PropertySelector propertySelector1 = (PropertySelector) list[0];
            PropertySelector propertySelector2 = (PropertySelector) list[1];
            QueryVertex vertex1 = null;
            QueryVertex vertex2 = null;
            if (triplet.getSourceVertex().hasVariable(propertySelector1.getVariable())) {
              vertex1 = triplet.getSourceVertex();
              if (triplet.getTargetVertex().hasVariable(propertySelector2.getVariable())) {
                vertex2 = triplet.getTargetVertex();
              }
            } else if (triplet.getTargetVertex().hasVariable(propertySelector1.getVariable())) {
              vertex1 = triplet.getTargetVertex();
              if (triplet.getSourceVertex().hasVariable(propertySelector2.getVariable())) {
                vertex2 = triplet.getSourceVertex();
              }
            }
            PropertyValue value1 = null;
            PropertyValue value2 = null;
            if (vertex1 != null && vertex2 != null) {
              value1 = vertex1.getPropertyValue(propertySelector1.getPropertyName());
              value2 = vertex2.getPropertyValue(propertySelector2.getPropertyName());
            }
            if (value1 != null && value2 != null) { // check here if right
              switch (comparison.getComparator()) {
                case EQ:
                  result = value1.compareTo(value2) == 0;
                  break;
                case NEQ:
                  result = value1.compareTo(value2) != 0;
                  break;
                case GT:
                  result = value1.compareTo(value2) > 0;
                  break;
                case LT:
                  result = value1.compareTo(value2) < 0;
                  break;
                case GTE:
                  result = value1.compareTo(value2) >= 0;
                  break;
                case LTE:
                  result = value1.compareTo(value2) <= 0;
                  break;
              }
            } else {
              return false; // found null instead of value to compare with
            }
          }  // inter vertex
        }
      } else {// last round was false
        return false;
      }
    }
    return true;
  }
}
