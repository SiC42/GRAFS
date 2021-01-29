package edu.leipzig.grafs.operators.interfaces.window;

import edu.leipzig.grafs.model.window.WindowsI;
import org.apache.flink.streaming.api.windowing.windows.Window;

/**
 * Interface for operators which takes a graph and produces one graph.
 * <p>
 * This is only a formal definition and not used in any other way than to provide a visual indicator
 * which operators do what. With the right application it can be possible to provide the operator
 * with graph collections and handle them well internally, however that is not build in
 * automatically.
 */
public interface WindowedGraphToGraphOperatorI<W extends WindowsI<? extends Window>> extends
    WindowedOperatorI<W> {

}
