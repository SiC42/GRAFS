package streaming.operators.grouping.functions;

import streaming.model.Vertex;
import streaming.operators.grouping.model.AggregatedVertex;
import streaming.operators.grouping.model.AggregationMapping;

public interface VertexAggregationProcess extends GraphElementAggregationProcess {

  default AggregatedVertex aggregateVertex(AggregationMapping aggregationMapping,
      AggregatedVertex aggregatedVertex,
      Vertex curVertex) {
    if (aggregatedVertex.isAlreadyAggregated(curVertex)) {
      return aggregatedVertex;
    } else {
      aggregatedVertex.addVertex(curVertex);
      return (AggregatedVertex) aggregateGraphElement(aggregationMapping, aggregatedVertex,
          curVertex);
    }
  }

}
