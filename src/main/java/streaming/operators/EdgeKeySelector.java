package streaming.operators;

import java.util.Collection;
import org.apache.flink.api.java.functions.KeySelector;
import streaming.model.Edge;
import streaming.model.GraphElementInformation;
import streaming.model.grouping.GroupingInformation;

public class EdgeKeySelector implements KeySelector<Collection<Edge>, String> {

  private GroupingInformation vertexEgi;
  private GroupingInformation edgeEgi;
  private AggregateMode makeKeyFor;

  public EdgeKeySelector(GroupingInformation vertexEgi, GroupingInformation edgeEgi,
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
      GroupingInformation vertexEgi) {
    StringBuilder sb = new StringBuilder();

    // Build Key based on Vertex Information
    sb = keyStringBuilder(sb, vertexGei, vertexEgi, "Vertex");

    return sb.toString();
  }

  private String generateKeyForEdge(Edge edge, GroupingInformation vertexEgi,
      GroupingInformation edgeEgi) {
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
      gei, GroupingInformation egi, String elementStr) {
    sb.append(elementStr).append("-Grouping-Information:(");
    if (egi.useLabel) {
      sb.append(String.format("label:%s ", gei.getLabel()));
    }
    if (egi.useMembership) {
      sb.append(String.format("membership:%d ", gei.getMembership()));
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
