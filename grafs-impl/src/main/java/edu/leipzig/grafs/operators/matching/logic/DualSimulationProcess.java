package edu.leipzig.grafs.operators.matching.logic;

import static java.util.stream.Collectors.toSet;

import edu.leipzig.grafs.factory.EdgeFactory;
import edu.leipzig.grafs.factory.VertexFactory;
import edu.leipzig.grafs.model.BasicTriplet;
import edu.leipzig.grafs.model.Edge;
import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.model.Vertex;
import edu.leipzig.grafs.operators.matching.model.Query;
import edu.leipzig.grafs.operators.matching.model.QueryEdge;
import edu.leipzig.grafs.operators.matching.model.QueryVertex;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.s1ck.gdl.model.comparables.ComparableExpression;
import org.s1ck.gdl.model.comparables.ElementSelector;
import org.s1ck.gdl.model.comparables.Literal;
import org.s1ck.gdl.model.comparables.PropertySelector;
import org.s1ck.gdl.model.predicates.Predicate;
import org.s1ck.gdl.model.predicates.booleans.And;
import org.s1ck.gdl.model.predicates.booleans.Not;
import org.s1ck.gdl.model.predicates.booleans.Or;
import org.s1ck.gdl.model.predicates.booleans.Xor;
import org.s1ck.gdl.model.predicates.expressions.Comparison;

/**
 * Applies a dual simulation algorithm (based on <a href="https://ieeexplore.ieee.org/abstract/document/6906821">"DualIso:
 * An Algorithm for Subgraph Pattern Matching on Very Large Labeled Graphs"</a> by Saltz et al.)
 *
 * @param <W> type of window used
 */
public class DualSimulationProcess<W extends Window> extends PatternMatchingProcess<W> {

  private final Query query;

  public DualSimulationProcess(Query query) {
    this.query = query;
  }

  /**
   * Evaluates the window and outputs none or several elements.
   *
   * @param context  The context in which the window is being evaluated.
   * @param elements The elements in the window being evaluated.
   * @param out      A collector for emitting elements.
   */
  @Override
  public void process(Context context, Iterable<BasicTriplet<QueryVertex, QueryEdge>> elements,
      Collector<Triplet> out) {
    Iterable<Triplet> triplets;
    if (query.getEdges().isEmpty()) {
      throw new RuntimeException(
          "Can't process query with only vertices, because only triplet stream model is supported");
    } else {
      triplets = executeForEdgesWithVertices(elements);
    }
    for (var triplet : triplets) {
      out.collect(triplet);
    }
  }

  private Iterable<Triplet> executeForEdgesWithVertices(
      Iterable<BasicTriplet<QueryVertex, QueryEdge>> elements) {
    // get unique elements from stream
    Set<QueryVertex> initialCandidateVertices = new HashSet<>(); // used for process
    Map<GradoopId, QueryVertex> idToVertexMap = new HashMap<>(); // used to find vertices for edges at the end
    Set<Edge> edgeSet = new HashSet<>();
    for (var candidate : elements) {
      var source = candidate.getSourceVertex();
      var target = candidate.getTargetVertex();
      initialCandidateVertices.add(source);
      initialCandidateVertices.add(target);
      idToVertexMap.put(source.getId(), source);
      idToVertexMap.put(target.getId(), target);
      edgeSet.add(candidate.getEdge());
    }
    // Copy to be able to delete during iteration; for optimisation
    Map<QueryVertex, GradoopId> prunableCandidateVertices = new HashMap<>();
    for (var v : initialCandidateVertices) {
      prunableCandidateVertices.put(v, v.getId());
    }

    for (var candidateVertex : initialCandidateVertices) {
      boolean stillMatch = checkParentsAndChildren(candidateVertex, query.toTriplets(),
          elements);// TODO: check with vertexMap.values()
            /*Map<String, Vertex> m = new HashMap<String, Vertex>(vertexMap);// to remove self
            m.remove(candidateVertex.getHash());*/
      if (!stillMatch || !checkPredicateTree(candidateVertex, query.getPredicates(),
          initialCandidateVertices)) {
        prunableCandidateVertices.remove(candidateVertex);
      }
    }

    var finalEdgeSet = edgeSet.stream()
        .filter(e -> prunableCandidateVertices.containsValue(e.getSourceId()))
        .filter(e -> prunableCandidateVertices.containsValue(e.getTargetId()))
        .collect(toSet());
    var result = new ArrayList<Triplet>();
    for (var edge : finalEdgeSet) {
      var source = idToVertexMap.get(edge.getSourceId());
      var target = idToVertexMap.get(edge.getTargetId());
      result.add(new Triplet(EdgeFactory.createEdge(edge), VertexFactory.createVertex(source),
          VertexFactory.createVertex(target)));
    }
    return result;
  }

  private boolean checkPredicateTree(QueryVertex currentCandidateVertex, Predicate predicates,
      Collection<QueryVertex> candidatesInWindow) {
    if (predicates.getArguments().length > 1) {
      boolean applyLeft = predicates.getArguments()[0].getVariables().stream()
          .anyMatch(currentCandidateVertex.getVariables()::contains);
      boolean applyRight = predicates.getArguments()[1].getVariables().stream()
          .anyMatch(currentCandidateVertex.getVariables()::contains);
      if (And.class.equals(predicates.getClass())) {
        if (applyLeft && applyRight) {
          return checkPredicateTree(currentCandidateVertex, predicates.getArguments()[0],
              candidatesInWindow) &&
              checkPredicateTree(currentCandidateVertex, predicates.getArguments()[1],
                  candidatesInWindow);
        } else if (applyLeft) {
          return checkPredicateTree(currentCandidateVertex, predicates.getArguments()[0],
              candidatesInWindow);
        } else {
          return checkPredicateTree(currentCandidateVertex, predicates.getArguments()[1],
              candidatesInWindow);
        }
      } else if (Not.class.equals(predicates.getClass())) {
        return !checkPredicateTree(currentCandidateVertex, predicates.getArguments()[0],
            candidatesInWindow);
      } else if (Or.class.equals(predicates.getClass())) {
        if (applyLeft && applyRight) {
          return checkPredicateTree(currentCandidateVertex, predicates.getArguments()[0],
              candidatesInWindow) ||
              checkPredicateTree(currentCandidateVertex, predicates.getArguments()[1],
                  candidatesInWindow);
        } else if (applyLeft) {
          return checkPredicateTree(currentCandidateVertex, predicates.getArguments()[0],
              candidatesInWindow);
        } else {
          checkPredicateTree(currentCandidateVertex, predicates.getArguments()[1],
              candidatesInWindow);
        }
      } else if (Xor.class.equals(predicates.getClass())) {
        if (applyLeft && applyRight) {
          return checkPredicateTree(currentCandidateVertex, predicates.getArguments()[0],
              candidatesInWindow) ^
              checkPredicateTree(currentCandidateVertex, predicates.getArguments()[1],
                  candidatesInWindow);
        } else if (applyLeft) {
          return checkPredicateTree(currentCandidateVertex, predicates.getArguments()[0],
              candidatesInWindow);
        } else {
          return checkPredicateTree(currentCandidateVertex, predicates.getArguments()[1],
              candidatesInWindow);
        }
      }
    } else if (Comparison.class
        .equals(predicates.getClass())) { // comparison has no further predicates
      Comparison comparison = (Comparison) predicates;
      ComparableExpression left = comparison.getComparableExpressions()[0];
      ComparableExpression right = comparison.getComparableExpressions()[1];
      if (comparison.getVariables().size() == 1) {
        return compareSingleVariable(currentCandidateVertex, comparison, left, right);
      } else {
        Collection<QueryVertex> streamVerticesToCompareWith = getVerticesToCompareWith(
            currentCandidateVertex, candidatesInWindow, comparison);
        if (left.getClass().equals(PropertySelector.class) && right.getClass().equals(
            PropertySelector.class)) {// TODO:exclude the case where it may be evaluated in edge self predicates
          return compareWithPropertySelector(currentCandidateVertex, comparison, left,
              (PropertySelector) right, streamVerticesToCompareWith);
        } else if (left.getClass().equals(ElementSelector.class) && right.getClass()
            .equals(ElementSelector.class)) {
          return compareWithElementSelector(currentCandidateVertex, comparison,
              streamVerticesToCompareWith);
        }
      }
    }
    return true;// no comparison? error? fall back to dual simulation?
  }

  private boolean compareSingleVariable(Vertex currentCandidateVertex, Comparison comparison,
      ComparableExpression left, ComparableExpression right) {
    PropertyValue leftValue = null;
    PropertyValue rightValue = null;
    if (left.getClass().equals(PropertySelector.class)) {// the other is Literal
      if (((PropertySelector) left).getPropertyName().equals("__label__")) {
        return true;
      }
      leftValue = currentCandidateVertex
          .getPropertyValue(((PropertySelector) left).getPropertyName());
      if (right.getClass().equals(Literal.class)) {
        rightValue = PropertyValue.create(((Literal) right).getValue());
      }
    } else if (left.getClass().equals(Literal.class)) {
      leftValue = PropertyValue.create(((Literal) left).getValue());
      if (right.getClass().equals(PropertySelector.class)) {
        if (((PropertySelector) right).getPropertyName().equals("__label__")) {
          return true;
        }
        rightValue = currentCandidateVertex
            .getPropertyValue(((PropertySelector) right).getPropertyName());
      }
    }
    return executeComparison(comparison, leftValue, rightValue);
    // return true; // as we are in window and all single predicate would be passes in the filter //TODO: check this
  }

  private Collection<QueryVertex> getVerticesToCompareWith(QueryVertex currentCandidateVertex,
      Collection<QueryVertex> candidatesInWindow, Comparison comparison) {
    String otherVariable = null;
    for (String var : comparison.getVariables()) {
      for (String candidateVariable : currentCandidateVertex.getVariables()) {
        if (!var.equals(candidateVariable)) {// TODO: check
          otherVariable = var;
          break;
        }
      }

    }
    String finalOtherVariable = otherVariable;
    return candidatesInWindow.stream().filter(element ->
        element.hasVariable(finalOtherVariable)).collect(Collectors.toList());
  }

  private boolean compareWithPropertySelector(QueryVertex currentCandidateVertex,
      Comparison comparison, ComparableExpression left, PropertySelector right,
      Collection<QueryVertex> streamVerticesToCompareWith) {
    PropertyValue leftValue;
    PropertyValue rightValue;
    if (currentCandidateVertex.hasVariable(left.getVariable())) {
      leftValue = currentCandidateVertex
          .getPropertyValue(((PropertySelector) left).getPropertyName());
      boolean result = false;
      for (QueryVertex opponent : streamVerticesToCompareWith) {
        rightValue = opponent.getPropertyValue(right.getPropertyName());
        if (leftValue != null && rightValue != null) {
          if (executeComparison(comparison, leftValue, rightValue)) {
            result = true;
            // currentCandidateVertex.getMatchingIds().add(opponent.getId());
          }
        }
      }
      return result;
    } else {
      leftValue = currentCandidateVertex
          .getPropertyValue(((PropertySelector) left).getPropertyName());
      boolean result = false;
      for (Vertex opponent : streamVerticesToCompareWith) {
        rightValue = opponent.getPropertyValue(right.getPropertyName());
        if (leftValue != null && rightValue != null) {
          if (executeComparison(comparison, rightValue, leftValue)) {
            result = true;
            // currentCandidateVertex.getMatchingIds().add(opponent.getId());
          }
        }
      }
      return result;
    }
  }

  private boolean compareWithElementSelector(QueryVertex currentCandidateVertex,
      Comparison comparison, Collection<QueryVertex> streamVerticesToCompareWith) {
    PropertyValue leftValue;
    PropertyValue rightValue;
    leftValue = PropertyValue.create(currentCandidateVertex.getId());
    boolean result = false;
    for (Vertex opponent : streamVerticesToCompareWith) {
      rightValue = PropertyValue.create(opponent.getId());
      if (executeComparison(comparison, rightValue, leftValue)) {
        result = true;
        // currentCandidateVertex.getMatchingIds().add(opponent.getId());
      }
    }
    return result;
  }

  private boolean executeComparison(Comparison comparison, PropertyValue leftValue,
      PropertyValue rightValue) {
    if (leftValue == null || rightValue == null) {
      return false;
    }
    switch (comparison.getComparator()) {
      case EQ:
        return leftValue.compareTo(rightValue) == 0;
      case NEQ:
        return leftValue.compareTo(rightValue) != 0;
      case GT:
        return leftValue.compareTo(rightValue) > 0;
      case LT:
        return leftValue.compareTo(rightValue) < 0;
      case GTE:
        return leftValue.compareTo(rightValue) >= 0;
      case LTE:
        return leftValue.compareTo(rightValue) <= 0;
      default:
        throw new IllegalStateException("Unexpected value: " + comparison.getComparator());
    }
  }

  private boolean checkParentsAndChildren(QueryVertex currentCandidateVertex,
      Collection<BasicTriplet<QueryVertex, QueryEdge>> queryTriples,
      Iterable<BasicTriplet<QueryVertex, QueryEdge>> candidatesInWindow) {
    java.util.function.Predicate<BasicTriplet<QueryVertex, QueryEdge>> oneVertexInTripletMatchesCurVertex = e ->
        ElementMatcher.matchesQueryElem(e.getSourceVertex(), currentCandidateVertex) ||
            ElementMatcher.matchesQueryElem(e.getTargetVertex(), currentCandidateVertex);

    var queryRelatives = queryTriples
        .stream()
        .filter(oneVertexInTripletMatchesCurVertex)
        .collect(Collectors.toList());
    List<BasicTriplet<QueryVertex, QueryEdge>> candidateRelatives = StreamSupport
        .stream(candidatesInWindow.spliterator(), false)
        .filter(oneVertexInTripletMatchesCurVertex)
        .collect(Collectors.toList());

    for (var relative : queryRelatives) {
      boolean exist = false;
      java.util.function.Predicate<BasicTriplet<QueryVertex, QueryEdge>> tripletMatchesRelative = t ->
          ElementMatcher.matchesQueryElem(relative.getSourceVertex(), t.getSourceVertex()) &&
              ElementMatcher.matchesQueryElem(relative.getTargetVertex(), t.getTargetVertex()) &&
              ElementMatcher.matchesQueryElem(relative.getEdge(), t.getEdge());
      for (var candidate : candidateRelatives) {
        exist = tripletMatchesRelative.test(candidate);
        if (exist) {
          break;
        }
      }
      if (!exist) {
        return false;
      }
    }
    return true;
  }

}
