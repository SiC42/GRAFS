package streaming.operators;

import java.util.Collection;
import org.apache.flink.api.java.functions.KeySelector;
import streaming.model.Edge;
import streaming.model.GraphElementInformation;
import streaming.model.grouping.ElementGroupingInformation;

public class EdgeKeySelector implements KeySelector<Collection<Edge>, String> {

  private ElementGroupingInformation vertexEgi;
  private ElementGroupingInformation edgeEgi;
  private AggregateMode makeKeyFor;

  public EdgeKeySelector(ElementGroupingInformation vertexEgi, ElementGroupingInformation edgeEgi,
      AggregateMode makeKeyFor) {
    this.vertexEgi = vertexEgi;
    this.edgeEgi = edgeEgi;
    this.makeKeyFor = makeKeyFor;
  }

  @Override
  public String getKey(Collection<Edge> edgeSet) {
    Edge edge = edgeSet.iterator().next();
    switch (makeKeyFor) {

      case SOURCE:
        return generateKeyForSingleVertex(edge.getSource().getGei(), vertexEgi);
      case TARGET:
        return generateKeyForSingleVertex(edge.getTarget().getGei(), vertexEgi);
      case EDGE:
        return generateKeyForEdge(edge, vertexEgi, edgeEgi);
    }
    return null;
  }


  private String generateKeyForSingleVertex(GraphElementInformation vertexGei,
      ElementGroupingInformation vertexEgi) {
    StringBuilder sb = new StringBuilder();

    // Build Key based on Vertex Information
    sb = keyStringBuilder(sb, vertexGei, vertexEgi, "Vertex");

    return sb.toString();
  }

  private String generateKeyForEdge(Edge edge, ElementGroupingInformation vertexEgi,
      ElementGroupingInformation edgeEgi) {
    StringBuilder sb = new StringBuilder();

    // Build Key based on Source Vertex Information
    sb = keyStringBuilder(sb, edge.getSource().getGei(), vertexEgi, "Source");

    // Build Key based on Edge Information
    if (edgeEgi != null) {
      sb = keyStringBuilder(sb, edge.getGei(), edgeEgi, "Edge");
    }

    // Build Key based on Target Vertex Information
    sb = keyStringBuilder(sb, edge.getTarget().getGei(), vertexEgi, "Target");

    return sb.toString();
  }

  private StringBuilder keyStringBuilder(StringBuilder sb, GraphElementInformation
      gei, ElementGroupingInformation egi, String elementStr) {
    sb.append(elementStr).append("-Grouping-Information:(");
    if (egi.shouldGroupByLabel) {
      sb.append(String.format("label:%s ", gei.getLabel()));
    }
    if (egi.shouldGroupByMembership) {
      sb.append(String.format("membership:%s ", gei.getMemberships().toString()));
    }
    if (!egi.groupingKeys.isEmpty()) {
      sb.append("properties:{");
      for (String key : egi.groupingKeys) {
        sb.append(String.format("(%s:%s) ", key, gei.getProperty(key)));
      }
      sb.append("}");
    }
    sb.append(")");
    return sb;
  }
}
