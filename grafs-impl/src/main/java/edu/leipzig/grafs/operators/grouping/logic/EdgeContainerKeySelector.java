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

  /**
   * Constructs the key in GDL format
   *
   * @param source generated String for source vertex
   * @param edge   generated String for edge
   * @param target generated String for target vertex
   * @return key for the whole edge container in GDL format
   */
  private String generateKey(String source, String edge, String target) {
    return String.format("%s-%s->%s", source, edge, target);
  }

  /**
   * Generates the key component for the vertex based on the grouping information.
   *
   * @param vertex   vertex for which the key should be generated
   * @param vertexGi grouping information for the vertex
   * @return key for the given vertex in GDL format based on the grouping information
   */
  private String generateKeyForVertex(Vertex vertex, GroupingInformation vertexGi) {
    StringBuilder sb = new StringBuilder();
    sb.append("(");
    sb = keyStringBuilder(sb, vertex, vertexGi);
    sb.append(")");
    return sb.toString();
  }

  /**
   * Generates the key component for the edge based on the grouping information.
   *
   * @param edge   edge for which the key should be generated
   * @param edgeGi grouping information for the edge
   * @return key for the given edge in GDL format based on the grouping information
   */
  private String generateKeyForEdge(Edge edge, GroupingInformation edgeGi) {
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    sb = keyStringBuilder(sb, edge, edgeGi);
    sb.append("]");
    return sb.toString();
  }

  /**
   * Fills key builder based on the element and the grouping information
   *
   * @param sb        string builder with right bracket already appended
   * @param element   element for which the key should be generated
   * @param groupInfo grouping information for the element
   * @return filled key builder with missing end bracket
   */
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

  /**
   * Builds a grouping information which contains all property keys and label of the element unique
   * to this element and all elements that share the same values and labels.
   *
   * @param element element for which the unique grouping information should be generated
   * @return unique grouping information for this element
   */
  private GroupingInformation createUniqueGroupInfo(Element element) {
    var keys = element.getPropertyKeys();
    var sortedKeySet = new TreeSet<String>();
    for (var key : keys) {
      sortedKeySet.add(key);
    }
    return new GroupingInformation(true, sortedKeySet);
  }
}
