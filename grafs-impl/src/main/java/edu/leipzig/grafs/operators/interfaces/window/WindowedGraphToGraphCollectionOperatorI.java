package edu.leipzig.grafs.operators.interfaces.window;

import edu.leipzig.grafs.model.streaming.GCStream;
import edu.leipzig.grafs.model.streaming.GraphStream;

/**
 * Interface for operators which takes a graph and produces multiple graphs (i.e. graph
 * collections). This is only a formal definition and not used in any other way than to provide a
 * visual indicator which operators do what.
 */
public interface WindowedGraphToGraphCollectionOperatorI extends
    WindowedOperatorI<GraphStream, GCStream> {

}
