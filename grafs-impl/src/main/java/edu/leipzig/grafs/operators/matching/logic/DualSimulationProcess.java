package edu.leipzig.grafs.operators.matching.logic;

import static java.util.stream.Collectors.toSet;

import edu.leipzig.grafs.factory.EdgeFactory;
import edu.leipzig.grafs.factory.VertexFactory;
import edu.leipzig.grafs.model.Edge;
import edu.leipzig.grafs.model.Element;
import edu.leipzig.grafs.model.Graph;
import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.model.Vertex;
import edu.leipzig.grafs.operators.matching.model.Query;
import edu.leipzig.grafs.operators.matching.model.QueryEdge;
import edu.leipzig.grafs.operators.matching.model.QueryVertex;
import edu.leipzig.grafs.util.MultiMap;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
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
   * @param context   The context in which the window is being evaluated.
   * @param elements  The elements in the window being evaluated.
   * @param collector A collector for emitting elements.
   */
  @Override
  public void process(Context context, Iterable<Triplet<QueryVertex, QueryEdge>> elements,
      Collector<Triplet<Vertex, Edge>> collector) {
    Iterable<Triplet<Vertex, Edge>> triplets;
    if (query.getEdges().isEmpty()) {
      throw new RuntimeException(
          "Can't process query with only vertices, because only triplet stream model is supported");
    } else {
      executeForEdgesWithVertices(elements, collector);
    }
  }

  private void executeForEdgesWithVertices(
      Iterable<Triplet<QueryVertex, QueryEdge>> tripletElements,
      Collector<Triplet<Vertex, Edge>> collector) {

    var windowGraph = extractWindowGraph(tripletElements);
    var variablesToElementMap = mapQueryVariablesToElements(windowGraph);
    dualSimulationAlgorithm(windowGraph, variablesToElementMap, collector);
  }



  private Graph<QueryVertex, QueryEdge> extractWindowGraph(Iterable<Triplet<QueryVertex, QueryEdge>> tripletElements){
    var vertexMap = new HashMap<GradoopId, QueryVertex>();
    var edgeMap = new HashMap<GradoopId, QueryEdge>();

    // Map triplets to above defined maps and assign the variables
    for (var triplet : tripletElements) {
      // source
      var source = triplet.getSourceVertex();
      var target = triplet.getTargetVertex();
      var edge = triplet.getEdge();
      var alreadyContained = vertexMap.get(source.getId());
      if (alreadyContained != null) {
        alreadyContained.addVariables(source.getVariables());
      } else {
        alreadyContained = source;
      }
      vertexMap.put(alreadyContained.getId(), alreadyContained);

      // target
      alreadyContained = vertexMap.get(target.getId());
      if (alreadyContained != null) {
        alreadyContained.addVariables(target.getVariables());
      } else {
        alreadyContained = target;
      }
      vertexMap.put(alreadyContained.getId(), alreadyContained);

      // edge
      var alreadyContainedEdge = edgeMap.get(edge.getId());
      if (alreadyContainedEdge != null) {
        alreadyContained.addVariables(target.getVariables());
      } else {
        alreadyContainedEdge = edge;
      }
      edgeMap.put(alreadyContainedEdge.getId(), alreadyContainedEdge);
    }

    return new Graph<>(vertexMap.values(), edgeMap.values());
  }
  private MultiMap<String, Element> mapQueryVariablesToElements(Graph<QueryVertex, QueryEdge> windowGraph) {
    var variablesToElementMap = new MultiMap<String, Element>();
    for (var vertex : windowGraph.getVertices()) {
      for (var variable : vertex.getVariables()) {
        variablesToElementMap.put(variable, vertex);
      }
    }
    for (var edge : windowGraph.getEdges()) {
      for (var variable : edge.getVariables()) {
        variablesToElementMap.put(variable, edge);
      }
    }
    return variablesToElementMap;
  }

  /**
   * Uses the algorithm from the above mentioned DualIso paper and adds query support via additional filtering.
   * @param windowGraph Graph on which the pattern matching should be applied
   * @param variablesToElementMap Multi map of query variables and the associated elements
   * @param collector A collector for emitting elements.
   */
  private void dualSimulationAlgorithm(Graph<QueryVertex, QueryEdge> windowGraph,
      MultiMap<String, Element> variablesToElementMap, Collector<Triplet<Vertex, Edge>> collector) {
    var changed = true;
    while (changed) {
      changed = false;
      for (var queryVertex : query.getVertices()) {
        var queryVariable = queryVertex.getVariable();
        for (var queryVertexTarget : query.getTargetForSourceVertex(queryVertex)) {
          Set<Element> candidatesForQueryTarget = new HashSet<>();
          var candidatesForQueryVertex = variablesToElementMap.get(queryVariable);
          var queryVertexTargetVariable = queryVertexTarget.getVariable();
          for (Iterator<Element> candidateIt = candidatesForQueryVertex.iterator();
              candidateIt.hasNext(); ) {
            var candidate = (QueryVertex) candidateIt.next();
            if (checkPredicateTree(candidate, queryVariable, query.getPredicates(),
                variablesToElementMap)) {
              // TODO: Edge checking probably goes somewhere here. Preparations are made, edge should be passable to checkPredicateTree
              var targetCandidates = variablesToElementMap
                  .get(queryVertexTargetVariable)
                  .stream()
                  .map(e -> (QueryVertex) e)
                  .filter(windowGraph.getTargetForSourceVertex(candidate)::contains).collect(toSet());
              targetCandidates = targetCandidates.stream()
                  .filter(v ->
                      checkPredicateTree(v, queryVertexTargetVariable, query.getPredicates(),
                          variablesToElementMap))
                  .collect(toSet());
              if (targetCandidates.isEmpty()) {
                candidateIt.remove();
                if (candidatesForQueryVertex.isEmpty()) {
                  return;
                }
                changed = true;
              }
              candidatesForQueryTarget.addAll(targetCandidates);
            } else {
              candidateIt.remove();
            }
          }
          if (candidatesForQueryTarget.isEmpty()) {
            return;
          }
          if (candidatesForQueryTarget.size() < variablesToElementMap.get(
              queryVertexTargetVariable).size()) {
            changed = true;
          }
          variablesToElementMap.retainAll(queryVertexTargetVariable, candidatesForQueryTarget);
        }
      }
    }

    var emittableTriplets = new HashSet<Triplet<Vertex, Edge>>();
    for (var qSource : query.getVertices()) {
      var candidatesForSource = variablesToElementMap.get(qSource.getVariable());
      for (var sourceCandidate : candidatesForSource) {
        for (var qTarget : query.getTargetForSourceVertex(qSource)) {
          var candidatesForTarget = variablesToElementMap
              .get(qTarget.getVariable())
              .stream()
              .map(e -> (QueryVertex) e)
              .filter(windowGraph.getTargetForSourceVertex((QueryVertex) sourceCandidate)::contains)
              .collect(toSet());
          for (var targetCandidate : candidatesForTarget) {
            var edges = windowGraph
                .getEdgesForVertices((QueryVertex) sourceCandidate, targetCandidate);
            for (var edge : edges) {
              var queryEdges = query.getEdgesForVertices(qSource, qTarget);
              if (queryEdges.stream().anyMatch(qe -> ElementMatcher.matchesQueryElem(qe, edge))) {
                var normalSource = VertexFactory.createVertex((QueryVertex) sourceCandidate);
                var normalTarget = VertexFactory.createVertex(targetCandidate);
                var normalEdge = EdgeFactory.createEdge(edge);
                // TODO: find a better way to guarantee, that all edges are emitted once
                emittableTriplets.add(new Triplet<>(normalEdge, normalSource, normalTarget));
              }
            }
          }
        }
      }
    }
    emittableTriplets.forEach(collector::collect);
  }

  private boolean checkPredicateTree(Element currentCandidate, String currentVariable,
      Predicate predicates,
      MultiMap<String, Element> variableToVerticesMap) {
    if (predicates == null) {
      return true;
    }
    if (predicates.getArguments().length > 1) {
      boolean applyLeft = predicates.getArguments()[0].getVariables().stream()
          .anyMatch(currentVariable::equals);
      boolean applyRight = predicates.getArguments()[1].getVariables().stream()
          .anyMatch(currentVariable::equals);
      if (And.class.equals(predicates.getClass())) {
        if (applyLeft && applyRight) {
          return checkPredicateTree(currentCandidate, currentVariable,
              predicates.getArguments()[0],
              variableToVerticesMap) &&
              checkPredicateTree(currentCandidate, currentVariable,
                  predicates.getArguments()[1],
                  variableToVerticesMap);
        } else if (applyLeft) {
          return checkPredicateTree(currentCandidate, currentVariable,
              predicates.getArguments()[0],
              variableToVerticesMap);
        } else {
          return checkPredicateTree(currentCandidate, currentVariable,
              predicates.getArguments()[1],
              variableToVerticesMap);
        }
      } else if (Not.class.equals(predicates.getClass())) {
        return !checkPredicateTree(currentCandidate, currentVariable,
            predicates.getArguments()[0],
            variableToVerticesMap);
      } else if (Or.class.equals(predicates.getClass())) {
        if (applyLeft && applyRight) {
          return checkPredicateTree(currentCandidate, currentVariable,
              predicates.getArguments()[0],
              variableToVerticesMap) ||
              checkPredicateTree(currentCandidate, currentVariable,
                  predicates.getArguments()[1],
                  variableToVerticesMap);
        } else if (applyLeft) {
          return checkPredicateTree(currentCandidate, currentVariable,
              predicates.getArguments()[0],
              variableToVerticesMap);
        } else {
          checkPredicateTree(currentCandidate, currentVariable, predicates.getArguments()[1],
              variableToVerticesMap);
        }
      } else if (Xor.class.equals(predicates.getClass())) {
        if (applyLeft && applyRight) {
          return checkPredicateTree(currentCandidate, currentVariable,
              predicates.getArguments()[0],
              variableToVerticesMap) ^
              checkPredicateTree(currentCandidate, currentVariable,
                  predicates.getArguments()[1],
                  variableToVerticesMap);
        } else if (applyLeft) {
          return checkPredicateTree(currentCandidate, currentVariable,
              predicates.getArguments()[0],
              variableToVerticesMap);
        } else {
          return checkPredicateTree(currentCandidate, currentVariable,
              predicates.getArguments()[1],
              variableToVerticesMap);
        }
      }
    } else if (Comparison.class
        .equals(predicates.getClass())) { // comparison has no further predicates
      Comparison comparison = (Comparison) predicates;
      if (!predicates.getVariables().contains(currentVariable)) {
        return true;
      }
      ComparableExpression left = comparison.getComparableExpressions()[0];
      ComparableExpression right = comparison.getComparableExpressions()[1];
      var rightVar = right.getVariable();
      if (comparison.getVariables().size() == 1 && left.getVariable().equals(currentVariable)) {
        return compareSingleVariable(currentCandidate, comparison, left, right);
      } else if (left.getVariable().equals(currentVariable) ||
          (rightVar != null && rightVar
              .equals(currentVariable))) { // We have to compare Vertices with each other
        var verticesToCompareWith = getVerticesToCompareWith(
            currentVariable, variableToVerticesMap, comparison);
        if (left.getClass().equals(PropertySelector.class) &&
            right.getClass().equals(PropertySelector.class)) {
          return compareWithPropertySelector(
              currentCandidate,
              currentVariable,
              comparison,
              left,
              (PropertySelector) right,
              verticesToCompareWith);
        } else if (left.getClass().equals(ElementSelector.class) &&
            right.getClass().equals(ElementSelector.class)) {
          return compareWithElementSelector(
              currentCandidate,
              comparison,
              verticesToCompareWith);
        }
      }
    }
    return true;// no comparison? error? fall back to dual simulation?
  }

  private boolean compareSingleVariable(Element currenCandidate, Comparison comparison,
      ComparableExpression left, ComparableExpression right) {
    PropertyValue leftValue = null;
    PropertyValue rightValue = null;
    if (left.getClass().equals(PropertySelector.class)) {// the other is Literal
      if (((PropertySelector) left).getPropertyName().equals("__label__")) {
        return true;
      }
      leftValue = currenCandidate
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
        rightValue = currenCandidate
            .getPropertyValue(((PropertySelector) right).getPropertyName());
      }
    }
    return executeComparison(comparison, leftValue, rightValue);
    // return true; // as we are in window and all single predicate would be passes in the filter //TODO: check this
  }

  private Collection<Element> getVerticesToCompareWith(String currentVariable,
      MultiMap<String, Element> variableToVerticeMap, Comparison comparison) {
    return comparison.getVariables().stream()
        .filter(v -> !currentVariable.equals(v))
        .flatMap(v -> variableToVerticeMap.get(v).stream())
        .collect(toSet());
  }

  private boolean compareWithPropertySelector(Element currentCandidateVertex,
      String currentVariable,
      Comparison comparison, ComparableExpression left, PropertySelector right,
      Collection<Element> streamVerticesToCompareWith) {
    PropertyValue leftValue;
    PropertyValue rightValue;
    if (currentVariable.equals(left.getVariable())) {
      leftValue = currentCandidateVertex
          .getPropertyValue(((PropertySelector) left).getPropertyName());
      boolean result = false;
      for (var opponent : streamVerticesToCompareWith) {
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
      for (var opponent : streamVerticesToCompareWith) {
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

  private boolean compareWithElementSelector(Element currentCandidateVertex,
      Comparison comparison, Collection<Element> streamVerticesToCompareWith) {
    PropertyValue leftValue;
    PropertyValue rightValue;
    leftValue = PropertyValue.create(currentCandidateVertex.getId());
    boolean result = false;
    for (var opponent : streamVerticesToCompareWith) {
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
      Collection<Triplet<QueryVertex, QueryEdge>> queryTriples,
      Iterable<Triplet<QueryVertex, QueryEdge>> candidatesInWindow) {
    java.util.function.Predicate<Triplet<QueryVertex, QueryEdge>> oneVertexInTripletMatchesCurVertex = e ->
        ElementMatcher.matchesQueryElem(e.getSourceVertex(), currentCandidateVertex) ||
            ElementMatcher.matchesQueryElem(e.getTargetVertex(), currentCandidateVertex);

    var queryRelatives = queryTriples
        .stream()
        .filter(oneVertexInTripletMatchesCurVertex)
        .collect(Collectors.toList());
    List<Triplet<QueryVertex, QueryEdge>> candidateRelatives = StreamSupport
        .stream(candidatesInWindow.spliterator(), false)
        .filter(t -> t.getSourceVertex().equals(currentCandidateVertex) ||
            t.getTargetVertex().equals(currentCandidateVertex))
        .collect(Collectors.toList());

    for (var relative : queryRelatives) {
      boolean exist = false;
      java.util.function.Predicate<Triplet<QueryVertex, QueryEdge>> tripletMatchesRelative = t ->
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
