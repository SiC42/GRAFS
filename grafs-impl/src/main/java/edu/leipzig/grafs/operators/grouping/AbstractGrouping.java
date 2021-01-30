package edu.leipzig.grafs.operators.grouping;

import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.model.window.WindowingInformation;
import edu.leipzig.grafs.model.window.WindowsI;
import edu.leipzig.grafs.operators.grouping.functions.AggregateFunction;
import edu.leipzig.grafs.operators.grouping.model.GroupingInformation;
import edu.leipzig.grafs.operators.interfaces.nonwindow.GraphToGraphOperatorI;
import edu.leipzig.grafs.operators.interfaces.window.WindowedGraphToGraphOperatorI;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.windows.Window;

public abstract class AbstractGrouping<W extends WindowsI<? extends Window>> implements
    WindowedGraphToGraphOperatorI<W> {

  protected final GroupingInformation vertexGi;
  protected final Set<AggregateFunction> vertexAggregateFunctions;
  protected final GroupingInformation edgeGi;
  protected final Set<AggregateFunction> edgeAggregateFunctions;


  /**
   * Constructs the operator with the given grouping information, aggregation functions and the
   * window.
   *
   * @param vertexGi                 Grouping information for the vertices
   * @param vertexAggregateFunctions Aggregation functions for the grouped vertices
   * @param edgeGi                   Grouping information for the edges
   * @param edgeAggregateFunctions   Aggregation functions for the grouped edges
   */
  public AbstractGrouping(GroupingInformation vertexGi,
      Set<AggregateFunction> vertexAggregateFunctions,
      GroupingInformation edgeGi, Set<AggregateFunction> edgeAggregateFunctions) {
    this.vertexGi = vertexGi;
    this.vertexAggregateFunctions = vertexAggregateFunctions;
    this.edgeGi = edgeGi;
    this.edgeAggregateFunctions = edgeAggregateFunctions;
  }

  /**
   * Constructs the operator with the given grouping information (given as set), aggregation
   * functions and the window.
   *
   * @param vertexGiSet              set of keys (with {@link GroupingInformation#LABEL_SYMBOL if
   *                                 the element should be grouping should be applied on the label}
   *                                 for the vertices
   * @param vertexAggregateFunctions Aggregation functions for the grouped vertices
   * @param edgeGiSet                set of keys (with {@link GroupingInformation#LABEL_SYMBOL if
   *                                 the element should be grouping should be applied on the label}
   *                                 for the edges
   * @param edgeAggregateFunctions   Aggregation functions for the grouped edges
   */
  public AbstractGrouping(Set<String> vertexGiSet, Set<AggregateFunction> vertexAggregateFunctions,
      Set<String> edgeGiSet, Set<AggregateFunction> edgeAggregateFunctions) {
    this(new GroupingInformation(vertexGiSet),
        vertexAggregateFunctions,
        new GroupingInformation(edgeGiSet),
        edgeAggregateFunctions);
  }

  /**
   * Applies the grouping operator onto the stream
   *
   * @param stream stream on which the operator should be applied
   * @return the stream with the grouping operator applied
   */
  @Override
  public <FW extends Window> DataStream<Triplet> execute(DataStream<Triplet> stream,
      WindowingInformation<FW> wi) {
    return groupBy(stream, wi);
  }

  abstract <FW extends Window> DataStream<Triplet> groupBy(DataStream<Triplet> stream, WindowingInformation<FW> wi);

  /**
   * Builder that provides an intuitive way to generate a {@link AllWindowGrouping}-object.
   */
  public static abstract class AbstractGroupingBuilder<W extends WindowsI<? extends Window>> {

    final GroupingInformation vertexGi;
    final Set<AggregateFunction> vertexAggFunctions;

    final GroupingInformation edgeGi;
    final Set<AggregateFunction> aggregateFunctions;

    /**
     * Constructs the initial state
     */
    public AbstractGroupingBuilder() {
      vertexGi = new GroupingInformation();
      vertexAggFunctions = new HashSet<>();
      edgeGi = new GroupingInformation();
      aggregateFunctions = new HashSet<>();
    }

    /**
     * Adds the given grouping key to the vertex grouping information
     *
     * @param vertexGroupingKey grouping key for vertices
     * @return the build with the given grouping key applied
     */
    public AbstractGroupingBuilder<W> addVertexGroupingKey(String vertexGroupingKey) {
      vertexGi.addKey(vertexGroupingKey);
      return this;
    }

    /**
     * Adds the given grouping keys to the vertex grouping information
     *
     * @param vertexGroupingKeys set of grouping keys for vertices
     * @return the build with the given grouping keys applied
     */
    public AbstractGroupingBuilder<W> addVertexGroupingKeys(Set<String> vertexGroupingKeys) {
      vertexGi.addKeys(vertexGroupingKeys);
      return this;
    }

    /**
     * Adds the given grouping key to the edge grouping information
     *
     * @param edgeGroupingKey grouping key for edges
     * @return the build with the given grouping key applied
     */
    public AbstractGroupingBuilder<W> addEdgeGroupingKey(String edgeGroupingKey) {
      edgeGi.addKey(edgeGroupingKey);
      return this;
    }

    /**
     * Adds the given grouping keys to the vertex grouping information
     *
     * @param edgeGroupingKeys grouping keys for edges
     * @return the build with the given grouping keys applied
     */
    public AbstractGroupingBuilder<W> addEdgeGroupingKeys(Set<String> edgeGroupingKeys) {
      edgeGi.addKeys(edgeGroupingKeys);
      return this;
    }

    /**
     * Define, if the vertex label shall be used for grouping vertices.
     *
     * @param useVertexLabel true, iff vertex label shall be used for grouping
     * @return this builder
     */
    public AbstractGroupingBuilder<W> useVertexLabel(boolean useVertexLabel) {
      vertexGi.useLabel(useVertexLabel);
      return this;
    }

    /**
     * Define, if the edge label shall be used for grouping edges.
     *
     * @param useEdgeLabel true, iff edge label shall be used for grouping
     * @return this builder
     */
    public AbstractGroupingBuilder<W> useEdgeLabel(boolean useEdgeLabel) {
      edgeGi.useLabel(useEdgeLabel);
      return this;
    }

    /**
     * Add an aggregate function which is applied on all vertices represented by a single super
     * vertex.
     *
     * @param aggregateFunction vertex aggregate mapping
     * @return this builder
     */
    public AbstractGroupingBuilder<W> addVertexAggregateFunction(AggregateFunction aggregateFunction) {
      Objects.requireNonNull(aggregateFunction, "Aggregate function must not be null");
      vertexAggFunctions.add(aggregateFunction);
      return this;
    }

    /**
     * Add an aggregate function which is applied on all edges represented by a single super edge.
     *
     * @param eFunctions edge aggregate mapping
     * @return this builder
     */
    public AbstractGroupingBuilder<W> addEdgeAggregateFunction(AggregateFunction eFunctions) {
      Objects.requireNonNull(eFunctions, "Aggregate function must not be null");
      aggregateFunctions.add(eFunctions);
      return this;
    }

    public abstract AbstractGrouping<W> build();
  }

  }
