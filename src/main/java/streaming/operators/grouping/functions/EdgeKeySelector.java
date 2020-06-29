package streaming.operators.grouping.functions;

import org.apache.flink.api.java.functions.KeySelector;
import streaming.model.EdgeContainer;
import streaming.model.Element;
import streaming.operators.grouping.model.GroupingInformation;

public class EdgeKeySelector implements KeySelector<EdgeContainer, String> {

  private final GroupingInformation vertexEgi;
  private final GroupingInformation edgeEgi;
  private final AggregateMode makeKeyFor;

  public EdgeKeySelector(GroupingInformation vertexEgi, GroupingInformation edgeEgi,
      AggregateMode makeKeyFor) {
    this.vertexEgi = vertexEgi;
    this.edgeEgi = edgeEgi;
    this.makeKeyFor = makeKeyFor;
  }

  @Override
  public String getKey(EdgeContainer ec) {
    switch (makeKeyFor) {

      case SOURCE:
        return generateKeyForSingleVertex(ec.getSourceVertex(), vertexEgi);
      case TARGET:
        return generateKeyForSingleVertex(ec.getTargetVertex(), vertexEgi);
      case EDGE:
        return generateKeyForEdge(ec, vertexEgi, edgeEgi);
    }
    return null;
  }


  private String generateKeyForSingleVertex(Element vertexGei,
      GroupingInformation vertexEgi) {
    StringBuilder sb = new StringBuilder();

    // Build Key based on Vertex Information
    sb = keyStringBuilder(sb, vertexGei, vertexEgi, "Vertex");

    return sb.toString();
  }

  private String generateKeyForEdge(EdgeContainer edgeContainer, GroupingInformation vertexEgi,
      GroupingInformation edgeEgi) {
    StringBuilder sb = new StringBuilder();

    // Build Key based on Source Vertex Information
    sb = keyStringBuilder(sb, edgeContainer.getSourceVertex(), vertexEgi, "Source");

    // Build Key based on Edge Information
    if (edgeEgi != null) {
      sb = keyStringBuilder(sb, edgeContainer.getEdge(), edgeEgi, "Edge");
    }

    // Build Key based on Target Vertex Information
    sb = keyStringBuilder(sb, edgeContainer.getTargetVertex(), vertexEgi, "Target");

    return sb.toString();
  }

  private StringBuilder keyStringBuilder(StringBuilder sb, Element
      gei, GroupingInformation egi, String elementStr) {
    sb.append(elementStr).append("-Grouping-Information:(");
    if (egi.useLabel) {
      sb.append(String.format("label:%s ", gei.getLabel()));
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
