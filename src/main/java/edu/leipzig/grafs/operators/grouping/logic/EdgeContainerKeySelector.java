package edu.leipzig.grafs.operators.grouping.logic;

import edu.leipzig.grafs.model.Edge;
import edu.leipzig.grafs.model.EdgeContainer;
import edu.leipzig.grafs.model.Element;
import edu.leipzig.grafs.model.Vertex;
import edu.leipzig.grafs.operators.grouping.model.AggregateMode;
import edu.leipzig.grafs.operators.grouping.model.GroupingInformation;
import java.util.Objects;
import java.util.TreeSet;
import org.apache.flink.api.java.functions.KeySelector;

/**
 * Key selector used for grouping that returns a string representation of an {@link EdgeContainer}
 * using the grouping information.
 */
public class EdgeContainerKeySelector implements KeySelector<EdgeContainer, String> {

  private final GroupingInformation vertexGi;
  private final GroupingInformation edgeGi;
  private final AggregateMode makeKeyFor;

  /**
   * Constructors the key selector using the given information.
   *
   * @param vertexGi   information on which the vertices should be grouped upon
   * @param edgeGi     information on which the edges should be grouped upon
   * @param makeKeyFor determines if the key should be made for the source vertex, target vertex or
   *                   edge
   */
  public EdgeContainerKeySelector(GroupingInformation vertexGi, GroupingInformation edgeGi,
      AggregateMode makeKeyFor) {
    this.vertexGi = Objects.requireNonNull(vertexGi, "grouping information for vertex was null");
    this.edgeGi = edgeGi;
    this.makeKeyFor = makeKeyFor;
  }

  /**
   * Constructs the key for the given edge container using the information provided in the
   * constructor.
   * <p>
   * Two edge container generate the same key, if the selected element (i.e. the element for which
   * the key is made for via {@link AggregateMode}) are in the same group using the grouping
   * information.
   * <p>
   * The default toString methods of the elements are used for this to ease debugging.
   *
   * @param ec edge container for which the key should be made
   * @return key for the edge container that represents the group of the selected element
   */
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
    return new GroupingInformation(true, sortedKeySet);
  }
}
