package edu.leipzig.grafs.operators.matching.logic;

import com.codahale.metrics.UniformReservoir;
import edu.leipzig.grafs.model.Triplet;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

public abstract class PatternMatchingProcess<W extends Window> extends ProcessAllWindowFunction<Triplet, Triplet, W> {
    transient Histogram histogram;

    @Override
    public void open(Configuration parameters) throws Exception {
      super.open(parameters);
      com.codahale.metrics.Histogram dropwizardHistogram =
          new com.codahale.metrics.Histogram(new UniformReservoir());
      this.histogram = getRuntimeContext()
          .getMetricGroup()
          .histogram("AbdHistogram", new DropwizardHistogramWrapper(dropwizardHistogram));
    }

    /**
     * Evaluates the window and outputs none or several elements.
     *
     * @param context  The context in which the window is being evaluated.
     * @param elements The elements in the window being evaluated.
     * @param out      A collector for emitting elements.
     * @throws Exception The function may throw exceptions to fail the program and trigger recovery.
     */
    public abstract void process(Context context, Iterable<Triplet> elements, Collector<Triplet> out) throws Exception;
  }

}
