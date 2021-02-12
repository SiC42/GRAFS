package edu.leipzig.grafs.operators.matching.model;

import edu.leipzig.grafs.model.Graph;
import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.util.AsciiGraphLoader;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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

public class Query extends Graph<QueryVertex, QueryEdge> implements Serializable {

  private Predicate predicates;
  private Map<String,QueryVertex> variableToVertexMap;

  public Query() {
    super();
    this.variableToVertexMap = new HashMap<>();
  }


  public Query(String queryString, String timestampKey, List<String> variableOrder) {
    // "g[(alice)-[e1:knows {since : 2014}]->(bob)]"
    super(GradoopId.get(), new ArrayList<>(), new ArrayList<>(), Set.of(), Set.of());
    this.variableToVertexMap = new HashMap<>();
    GDLHandler handler = initQueryHandler(queryString);
    extractGraph(handler, timestampKey, variableOrder);
    if (timestampKey != null) {
      addOrderToPredicates(timestampKey);
    }
  }

  private void addOrderToPredicates(String timestampKey) {
    Comparison comparison;
    var edgeList = (ArrayList<QueryEdge>) edges;
    Predicate newPredicates = this.getPredicates();
    for (int i = 0; i < edges.size() - 1; i++) {
      QueryEdge e = edgeList.get(i);
      QueryEdge eNext = edgeList.get(i + 1);
      if (e.getOrder() < eNext.getOrder()) {
        comparison = new Comparison(new PropertySelector(e.getVariable(), timestampKey),
            Comparator.LT,
            new PropertySelector(eNext.getVariable(), timestampKey));
      } else {
        comparison = new Comparison(new PropertySelector(e.getVariable(), timestampKey),
            Comparator.GT,
            new PropertySelector(eNext.getVariable(), timestampKey));
      }
      newPredicates = new And(newPredicates, comparison);
    }
    if (newPredicates != null) {
      this.predicates = newPredicates;
    }
  }

  public boolean hasPredicates() {
    return predicates != null;
  }

  public Predicate getPredicates() {
    return predicates;
  }

  private void setPredicates(Predicate predicate) {
    this.predicates = predicate;
  }

  public Collection<Triplet<QueryVertex, QueryEdge>> toTriplets() {
    var result = new ArrayList<Triplet<QueryVertex, QueryEdge>>();
    for (var edge : edges) {
      var source = vertexMap.get(edge.getSourceId());
      var target = vertexMap.get(edge.getTargetId());
      result.add(new Triplet<>(edge, source, target));
    }
    return result;
  }

  private GDLHandler initQueryHandler(String queryString) {
    var handler = AsciiGraphLoader.createDefaultGdlHandlerBuilder().buildFromString(queryString);
    handler.getPredicates().ifPresent(this::setPredicates);
    return handler;
  }

  private void extractGraph(GDLHandler handler, String timestampKey,
      List<String> variableOrder) {
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
      if (timestampKey != null) {
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

  private void searchTreeForProperty(GraphElement elem, HasPredicate predicateElement,
      Predicate pTree) {
    if (pTree.getClass() == And.class) {// comparison?
      for (Predicate pSubTree : pTree.getArguments()) {
        searchTreeForProperty(elem, predicateElement, pSubTree);
      }
    }
    if (pTree.getVariables().contains(elem.getVariable())) {
      if (pTree.getClass() == Comparison.class) {
        if (pTree.getVariables().size() == 1) { // has info only about this object
          Comparison ca = (Comparison) pTree;
          if (ca.getComparator().equals(Comparator.EQ)) {
            if (ca.getComparableExpressions()[0].getClass() == PropertySelector.class &&
                ca.getComparableExpressions()[1].getClass() == Literal.class) {
              PropertySelector ps = (PropertySelector) ca.getComparableExpressions()[0];
              Literal ls = (Literal) ca.getComparableExpressions()[1];
              String propertyName = ps.getPropertyName();
              Object propertyValue = ls.getValue();
              if (!propertyName.equals("__label__")) {
                predicateElement.setProperty(propertyName, propertyValue);
              }
            }
          } else {
            predicateElement.addPredicate(pTree);
          }
        } //was here
      }
    } else if (pTree.getVariables().size() == 2 && predicateElement.getClass()
        == QueryEdge.class) { // edge: then look for direct neighbor expressions
      Optional<QueryVertex> relatedSourceVertex = vertices.stream().filter(element ->
          element.getId().equals(((QueryEdge) predicateElement).getSourceId())).findAny();
      Optional<QueryVertex> relatedTargetVertex = vertices.stream().filter(element ->
          element.getId().equals(((QueryEdge) predicateElement).getTargetId())).findAny();
      if (relatedSourceVertex.isPresent() && relatedTargetVertex.isPresent()) {
        Collection<String> variables = new ArrayList<>();
        variables.add(relatedTargetVertex.get().getVariable());
        variables.add(relatedSourceVertex.get().getVariable());
        if (pTree.getVariables().containsAll(variables)) {
          predicateElement.addPredicate(pTree);
        }
      }
    }
  }

  public boolean isEmpty() {
    return edges.size() == 0 && this.vertices.size() == 0;
  }


  public QueryVertex getVertexByVariable(String variable) {
    return variableToVertexMap.get(variable);
  }

  /**
   * Adds a vertex to the graph. Returns <tt>true</tt> if this vertex was not already part of the
   * graph. Does not add the vertex, if it is already part of the graph.
   *
   * @param vertex vertex to be added to this graph
   * @return <tt>true</tt>  if this graph did not already contain the specified vertex
   */
  public boolean addVertex(QueryVertex vertex) {
    var alreadyAdded = super.addVertex(vertex);
    this.variableToVertexMap.put(vertex.getVariable(), vertex);
    return alreadyAdded;
  }


}
