package edu.leipzig.grafs.operators.interfaces.window;

import edu.leipzig.grafs.model.streaming.GCStream;
import edu.leipzig.grafs.model.streaming.GraphStream;

public interface WindowedGraphCollectionToGraphOperatorI extends
    WindowedOperatorI<GCStream, GraphStream> {

}
