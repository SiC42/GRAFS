package edu.leipzig.grafs.operators.grouping.logic;

import edu.leipzig.grafs.exceptions.KeyOverlapException;
import edu.leipzig.grafs.model.Edge;
import edu.leipzig.grafs.model.Element;
import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.model.Vertex;
import edu.leipzig.grafs.operators.grouping.functions.AggregateFunction;
import edu.leipzig.grafs.operators.grouping.model.GroupingInformation;
import java.util.Set;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.gradoop.common.model.impl.properties.PropertyValue;

/**
 * Abstract class that provides basic functionalities for aggregating on elements in an edge
 * stream.
 *
 * @param <W> the type of window to be used for the grouping
 */
public abstract class ElementAggregation<W extends Window> extends
    ProcessWindowFunction<Triplet<Vertex, Edge>, Triplet<Vertex, Edge>, String, W> implements ElementAggregationI{

}
