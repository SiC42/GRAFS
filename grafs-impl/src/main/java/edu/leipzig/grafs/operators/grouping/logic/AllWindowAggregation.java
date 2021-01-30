package edu.leipzig.grafs.operators.grouping.logic;

import edu.leipzig.grafs.factory.EdgeFactory;
import edu.leipzig.grafs.model.Edge;
import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.model.Vertex;
import edu.leipzig.grafs.operators.grouping.functions.AggregateFunction;
import edu.leipzig.grafs.operators.grouping.model.GroupingInformation;
import edu.leipzig.grafs.util.MultiMap;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;

public class AllWindowAggregation<W extends Window> extends
    ProcessAllWindowFunction<Triplet, Triplet, W> implements ElementAggregationI {

  protected final GroupingInformation vertexGroupInfo;
  protected final Set<AggregateFunction> vertexAggregateFunctions;
  protected final GroupingInformation edgeGroupInfo;
  protected final Set<AggregateFunction> edgeAggregateFunctions;

  /**
   * Constructs the operator with the given grouping information, aggregation functions and the
   * window.
   *
   * @param vertexGroupInfo          Grouping information for the vertices
   * @param vertexAggregateFunctions Aggregation functions for the grouped vertices
   * @param edgeGroupInfo            Grouping information for the edges
   * @param edgeAggregateFunctions   Aggregation functions for the grouped edges
   */
  public AllWindowAggregation(
      GroupingInformation vertexGroupInfo, Set<AggregateFunction> vertexAggregateFunctions,
      GroupingInformation edgeGroupInfo, Set<AggregateFunction> edgeAggregateFunctions) {
    this.vertexGroupInfo = vertexGroupInfo;
    this.vertexAggregateFunctions = vertexAggregateFunctions;
    this.edgeGroupInfo = edgeGroupInfo;
    this.edgeAggregateFunctions = edgeAggregateFunctions;
  }


  @Override
  public void process(Context context, Iterable<Triplet> triplets, Collector<Triplet> collector)
      throws Exception {
    GradoopId newGraphId = GradoopId.get();
    MultiMap<GradoopId, GradoopId> sourceVertexToEdgeMap = new MultiMap<>();
    MultiMap<GradoopId, GradoopId> targetVertexToEdgeMap = new MultiMap<>();
    Map<GradoopId,Edge> edges = new HashMap<>();
    Map<GradoopId, Vertex> vertices = new HashMap<>();
    for (var triplet : triplets) {
      var source = triplet.getSourceVertex();
      vertices.put(source.getId(),source);
      var target = triplet.getTargetVertex();
      vertices.put(target.getId(), target);

      var edge = triplet.getEdge();
      edges.put(edge.getId(), edge);
      sourceVertexToEdgeMap.put(edge.getSourceId(), edge.getId());
      targetVertexToEdgeMap.put(edge.getTargetId(), edge.getId());
    }
    aggregateVertices(vertices, edges, sourceVertexToEdgeMap, targetVertexToEdgeMap, newGraphId);
    aggregateEdges(vertices, edges, newGraphId, collector);

  }

  private void aggregateEdges(Map<GradoopId, Vertex> vertices, Map<GradoopId,Edge> edges, GradoopId newGraphId, Collector<Triplet> collector) {
    MultiMap<String, Edge> groupedEdgesMap = new MultiMap<>();
    for (var edge : edges.values()) {
      var vKey = TripletKeySelector.generateKeyForEdge(edge, edgeGroupInfo);
      groupedEdgesMap.put(vKey, edge);
    }
    for(var key : groupedEdgesMap.keySet()) {
      Edge lastEdge = null;
      var aggregatedEdge = EdgeFactory.createEdge();
      for (var edge : groupedEdgesMap.get(key)) {
        aggregatedEdge = (Edge) aggregateElement(aggregatedEdge, edge, edgeAggregateFunctions);
        lastEdge = edge;
        edges.remove(edge.getId());
      }
      aggregatedEdge = (Edge) checkForMissingAggregationsAndApply(edgeAggregateFunctions,
          aggregatedEdge);

      aggregatedEdge = (Edge) setGroupedProperties(edgeGroupInfo, aggregatedEdge, lastEdge);
      var source = lastEdge.getSourceId();
      var target = lastEdge.getTargetId();
      aggregatedEdge.setSourceId(source);
      aggregatedEdge.setTargetId(target);
      aggregatedEdge.setGraphIds(GradoopIdSet.fromExisting(newGraphId));
      emitTriplet(collector, vertices, aggregatedEdge);
    }
  }

  private void emitTriplet(Collector<Triplet> collector, Map<GradoopId, Vertex> vertices,
      Edge e) {
      var source = vertices.get(e.getSourceId());
      var target = vertices.get(e.getTargetId());
      collector.collect(new Triplet(e, source, target));
  }

  private void aggregateVertices(Map<GradoopId, Vertex> vertices,
      Map<GradoopId,Edge> edges, MultiMap<GradoopId, GradoopId> sourceVertexToEdgeMap,
      MultiMap<GradoopId, GradoopId> targetVertexToEdgeMap,
      GradoopId newGraphId) {
    MultiMap<String, Vertex> groupedVerticesMap = new MultiMap<>();

    for (var vertex : vertices.values()) {
      var vKey = TripletKeySelector.generateKeyForVertex(vertex, vertexGroupInfo);
      groupedVerticesMap.put(vKey, vertex);
    }
    for (var key : groupedVerticesMap.keySet()) {
      var aggregatedVertex = new Vertex();

      // determine the aggregated vertice
      var isInitialAggregation = true;
      var groupedVertices = groupedVerticesMap.get(key);
      for (var curVertex : groupedVertices) {
        if (isInitialAggregation) {
          isInitialAggregation = false;
          aggregatedVertex = (Vertex) setGroupedProperties(vertexGroupInfo,
              aggregatedVertex, curVertex);
        }
        aggregatedVertex = (Vertex) aggregateElement(aggregatedVertex, curVertex,
            vertexAggregateFunctions);

        // update edges
        var edgesForSource = sourceVertexToEdgeMap.get(curVertex.getId());
        updateSourceForEdges(edges, aggregatedVertex, edgesForSource);
        var edgesForTarget = targetVertexToEdgeMap.get(curVertex.getId());
        updateTargetForEdges(edges, aggregatedVertex, edgesForTarget);
        vertices.remove(curVertex.getId());
      }
      aggregatedVertex = (Vertex) checkForMissingAggregationsAndApply(vertexAggregateFunctions,
          aggregatedVertex);
      aggregatedVertex.setGraphIds(GradoopIdSet.fromExisting(newGraphId));
      vertices.put(aggregatedVertex.getId(), aggregatedVertex);
    }
  }

  private void updateSourceForEdges(Map<GradoopId,Edge> edges, Vertex aggregatedVertex, Set<GradoopId> edgesForVertex) {
    for (var id : edgesForVertex) {
      var e = edges.get(id);
      var newEdge = EdgeFactory
          .initEdge(e.getId(), e.getLabel(), aggregatedVertex.getId(), e.getTargetId(),
              e.getProperties(), e.getGraphIds());
      edges.remove(e.getId());
      edges.put(newEdge.getId(),newEdge);
    }
  }

  private void updateTargetForEdges(Map<GradoopId,Edge> edges, Vertex aggregatedVertex, Set<GradoopId> edgesForVertex) {
    for (var id : edgesForVertex) {
      var e = edges.get(id);
      var newEdge = EdgeFactory
          .initEdge(e.getId(), e.getLabel(), e.getSourceId(), aggregatedVertex.getId(), e.getProperties(), e.getGraphIds());
      edges.remove(e.getId());
      edges.put(newEdge.getId(),newEdge);
    }
  }

}
