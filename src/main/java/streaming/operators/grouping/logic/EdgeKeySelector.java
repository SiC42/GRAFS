package streaming.operators.grouping.logic;

import java.util.Objects;
import java.util.TreeSet;
import org.apache.flink.api.java.functions.KeySelector;
import streaming.model.Edge;
import streaming.model.EdgeContainer;
import streaming.model.Element;
import streaming.model.Vertex;
import streaming.operators.grouping.model.AggregateMode;
import streaming.operators.grouping.model.GroupingInformation;

public class EdgeKeySelector implements KeySelector<EdgeContainer, String> {

  private final GroupingInformation vertexGi;
  private final GroupingInformation edgeGi;
  private final AggregateMode makeKeyFor;

  public EdgeKeySelector(GroupingInformation vertexGi, GroupingInformation edgeGi,
      AggregateMode makeKeyFor) {
    this.vertexGi = Objects.requireNonNull(vertexGi, "grouping information for vertex was null");
    this.edgeGi = edgeGi;
    this.makeKeyFor = makeKeyFor;
  }

  @Override
  public String getKey(EdgeContainer ec) {
    final String EMPTY_VERTEX = "()";
    final String EMPTY_EDGE = "[]";
    switch (makeKeyFor) {
      case SOURCE: {
        String vertex = generateKeyForVertex(ec.getSourceVertex(), vertexGi);
        return generateKey(vertex, EMPTY_EDGE, EMPTY_VERTEX);
      }
      case TARGET: {
        String vertex = generateKeyForVertex(ec.getTargetVertex(), vertexGi);
        return generateKey(EMPTY_VERTEX, EMPTY_EDGE, vertex);
      }
      case EDGE: {
        String source = generateKeyForVertex(ec.getSourceVertex(), vertexGi);
        String target = generateKeyForVertex(ec.getTargetVertex(), vertexGi);
        String edge = generateKeyForEdge(ec.getEdge(), edgeGi);
        return generateKey(source, edge, target);
      }
      default:
        throw new IllegalArgumentException("aggregate mode couldn't be found");
    }
  }

  private String generateKey(String source, String edge, String target) {
    return String.format("%s-%s->%s", source, edge, target);
  }

  private String generateKeyForVertex(Vertex sourceVertex, GroupingInformation vertexGi) {
    StringBuilder sb = new StringBuilder();

    // Build Key based on vertex Information
    sb.append("(");
    sb = keyStringBuilder(sb, sourceVertex, vertexGi);
    sb.append(")");
    return sb.toString();
  }

  private String generateKeyForEdge(Edge edge, GroupingInformation edgeGi) {
    StringBuilder sb = new StringBuilder();

    // Build Key based on vertex Information
    sb.append("[");
    sb = keyStringBuilder(sb, edge, edgeGi);
    sb.append("]");
    return sb.toString();
  }

  private StringBuilder keyStringBuilder(StringBuilder sb, Element
      element, GroupingInformation groupInfo) {
    if (groupInfo == null) {
      groupInfo = createUniqueGroupInfo(element);
    }
    if (groupInfo.shouldUseLabel()) {
      sb.append(String.format(":%s", element.getLabel()));
      if (!groupInfo.getKeys().isEmpty()) {
        sb.append(" ");
      }
    }
    if (!groupInfo.getKeys().isEmpty()) {
      sb.append("{");
      for (String key : groupInfo.getKeys()) {
        sb.append(String.format("%s:%s,", key, element.getPropertyValue(key)));
      }
      sb.replace(sb.length() - 1, sb.length(), "}");
    }
    return sb;
  }

  private GroupingInformation createUniqueGroupInfo(Element element) {
    var keys = element.getPropertyKeys();
    var sortedKeySet = new TreeSet<String>();
    for (var key : keys) {
      sortedKeySet.add(key);
    }
    return new GroupingInformation(true, true, sortedKeySet);
  }
}
