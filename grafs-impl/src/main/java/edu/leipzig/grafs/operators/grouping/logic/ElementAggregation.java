package edu.leipzig.grafs.operators.grouping.logic;

import edu.leipzig.grafs.model.Edge;
import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.model.Vertex;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;

/**
 * Abstract class that provides basic functionalities for aggregating on elements in an edge
 * stream.
 *
 * @param <W> the type of window to be used for the grouping
 */
public abstract class ElementAggregation<E extends Edge, W extends Window> extends
    ProcessWindowFunction<Triplet<Vertex, E>, Triplet<Vertex, E>, String, W> implements
    ElementAggregationI {

}
