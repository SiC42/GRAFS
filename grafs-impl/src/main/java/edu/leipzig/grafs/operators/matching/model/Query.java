package edu.leipzig.grafs.operators.matching.model;

import edu.leipzig.grafs.model.BasicGraph;
import edu.leipzig.grafs.model.BasicTriplet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Properties;
import org.s1ck.gdl.GDLHandler;
import org.s1ck.gdl.model.Edge;
import org.s1ck.gdl.model.GraphElement;
import org.s1ck.gdl.model.Vertex;
import org.s1ck.gdl.model.comparables.Literal;
import org.s1ck.gdl.model.comparables.PropertySelector;
import org.s1ck.gdl.model.predicates.Predicate;
import org.s1ck.gdl.model.predicates.booleans.And;
import org.s1ck.gdl.model.predicates.expressions.Comparison;
import org.s1ck.gdl.utils.Comparator;

public class Query extends BasicGraph<QueryVertex, QueryEdge> {

  private Predicate predicates;

  public Query() {
    super();
  }


  public Query(String queryString, boolean withEdgeOrder, List<String> variableOrder) {
    // "g[(alice)-[e1:knows {since : 2014}]->(bob)]"
    super(GradoopId.get(), new ArrayList<>(), new ArrayList<>(), Set.of(), Set.of());
    GDLHandler handler = initQueryHandler(queryString);
    extractGraph(handler, withEdgeOrder, new ArrayList<>(variableOrder));
    if (withEdgeOrder) {
      addOrderToPredicates(handler);
    }
  }

  private void addOrderToPredicates(GDLHandler handler) {
    Comparison comparison;
    var edgeList = (ArrayList<QueryEdge>) edges;
    Predicate newPredicates = this.getPredicates();
    for (int i = 0; i < edges.size() - 1; i++) {
      QueryEdge e = edgeList.get(i);
      QueryEdge eNext = edgeList.get(i + 1);
      if (e.getOrder() < eNext.getOrder()) {
        comparison = new Comparison(new PropertySelector(e.getVariable(), "timestamp"),
            Comparator.LT,
            new PropertySelector(eNext.getVariable(), "timestamp"));
      } else {
        comparison = new Comparison(new PropertySelector(e.getVariable(), "timestamp"),
            Comparator.GT,
            new PropertySelector(eNext.getVariable(), "timestamp"));
      }
      newPredicates = new And(newPredicates, comparison);
    }
    if (newPredicates != null) {
      this.predicates = newPredicates;
    }
  }

  public Predicate getPredicates() {
    return predicates;
  }

  public Collection<BasicTriplet<QueryVertex, QueryEdge>> toTriplets() {
    var result = new ArrayList<BasicTriplet<QueryVertex, QueryEdge>>();
    for (var edge : edges) {
      var source = vertexMap.get(edge.getSourceId());
      var target = vertexMap.get(edge.getTargetId());
      result.add(new BasicTriplet<>(edge, source, target));
    }
    return result;
  }

  private GDLHandler initQueryHandler(String queryString) {
    GDLHandler handler = new GDLHandler.Builder().buildFromString(queryString);
    handler.getPredicates().ifPresent(this::SetPredicates);
    return handler;
  }

  private void extractGraph(GDLHandler handler, boolean withEdgeOrder,
      ArrayList<String> variableOrder) {
    Map<Long, GradoopId> idMapping = new HashMap<>();
    for (Vertex v : handler.getVertices()) {
      var vId = GradoopId.get();
      idMapping.put(v.getId(), vId);
      QueryVertex tempVertex = new QueryVertex(vId, v.getLabel(),
          Properties.createFromMap(v.getProperties()), v.getVariable());
      // String id, String label, Properties properties
      if (this.predicates != null) {
        for (Predicate a : this.predicates.getArguments()) {
          searchTreeForProperty(v, tempVertex, a);
        }
      }
      addVertex(tempVertex);
    }

    for (Edge e : handler.getEdges()) {
      var edgeId = GradoopId.get();
      var sourceId = GradoopId.get();
      var targetId = GradoopId.get();
      QueryEdge tempEdge = new QueryEdge(edgeId, e.getLabel(), idMapping.get(e.getSourceVertexId()),
          idMapping.get(e.getTargetVertexId()), Properties.createFromMap(e.getProperties()),
          e.getVariable());
      // tempEdge.setOrder(variableOrder.indexOf(e.getVariable()));
      if (withEdgeOrder) {
        tempEdge.setOrder(variableOrder.indexOf(e.getVariable()));
      }
      if (this.predicates != null) {
        for (Predicate a : this.predicates.getArguments()) {
          searchTreeForProperty(e, tempEdge, a);
        }
      }

      addEdge(tempEdge);
    }
  }

  private void searchTreeForProperty(GraphElement v, HasPredicate tempVertex, Predicate a) {
    if (a.getClass() == And.class) {// comparison?
      for (Predicate aa : a.getArguments()) {
        searchTreeForProperty(v, tempVertex, aa);
      }
    }
    if (a.getVariables().contains(v.getVariable())) {
      if (a.getClass() == Comparison.class) {
        if (a.getVariables().size() == 1) { // has info only about this object
          Comparison ca = (Comparison) a;
          if (ca.getComparator().equals(Comparator.EQ)) {
            if (ca.getComparableExpressions()[0].getClass() == PropertySelector.class &&
                ca.getComparableExpressions()[1].getClass() == Literal.class) {
              PropertySelector ps = (PropertySelector) ca.getComparableExpressions()[0];
              Literal ls = (Literal) ca.getComparableExpressions()[1];
              String propertyName = ps.getPropertyName();
              Object propertyValue = ls.getValue();
              if (!propertyName.equals("__label__")) {
                tempVertex.setProperty(propertyName, propertyValue);
              }
            }
          } else {
            tempVertex.addPredicate(a);
          }
        } //was here
      }
    } else if (a.getVariables().size() == 2 && tempVertex.getClass()
        == QueryEdge.class) { // edge: then look for direct neighbor expressions
      Optional<QueryVertex> relatedSourceVertex = vertices.stream().filter(element ->
          element.getId().equals(((QueryEdge) tempVertex).getSourceId())).findAny();
      Optional<QueryVertex> relatedTargetVertex = vertices.stream().filter(element ->
          element.getId().equals(((QueryEdge) tempVertex).getTargetId())).findAny();
      if (relatedSourceVertex.isPresent() && relatedTargetVertex.isPresent()) {
        Collection<String> variables = new ArrayList<>();
        variables.add(relatedTargetVertex.get().getVariable());
        variables.add(relatedSourceVertex.get().getVariable());
        if (a.getVariables().containsAll(variables)) {
          tempVertex.addPredicate(a);
        }
      }
    }
  }

  private void SetPredicates(Predicate predicate) {
    this.predicates = predicate;
  }

  private boolean queryPredicatesOnlyAnd(Predicate predicate) {
    boolean result;
    if (predicate.getClass() != And.class && predicate.getClass() != Comparison.class) {
      return false;
    } else {
      result = true;
      for (Predicate p : predicate.getArguments()) {
        result = result && queryPredicatesOnlyAnd(p);
      }
    }
    return result;
  }

  public boolean isVertexOnly() {
    if (!this.isEmpty()) {
      return edges.size() == 0;
    }
    return false;
  }

  public boolean isEmpty() {
    return edges.size() == 0 && this.vertices.size() == 0;
  }


}
