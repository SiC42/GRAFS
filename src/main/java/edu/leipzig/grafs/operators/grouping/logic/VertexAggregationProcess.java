package edu.leipzig.grafs.operators.grouping.logic;

import edu.leipzig.grafs.model.Vertex;
import edu.leipzig.grafs.operators.grouping.model.AggregatedVertex;
import edu.leipzig.grafs.operators.grouping.model.AggregationMapping;

interface VertexAggregationProcess extends GraphElementAggregationProcess {

  default AggregatedVertex aggregateVertex(AggregationMapping aggregationMapping,
      AggregatedVertex aggregatedVertex,
      Vertex curVertex) {
    if (curVertex instanceof AggregatedVertex) {
      var curAggVertex = (AggregatedVertex) curVertex;
      for (var id : curAggVertex.getAggregatedVertexIds()) {
        if (!aggregatedVertex.addVertex(id)) {
          aggregatedVertex.addVertex(curVertex); // add current vertex, because it is an aggregation
        }
      }
    }
    if (aggregatedVertex.isAlreadyAggregated(curVertex)) {
      return aggregatedVertex;
    } else {
      aggregatedVertex.addVertex(curVertex);
      return (AggregatedVertex) aggregateGraphElement(aggregationMapping, aggregatedVertex,
          curVertex);
    }
  }

}
