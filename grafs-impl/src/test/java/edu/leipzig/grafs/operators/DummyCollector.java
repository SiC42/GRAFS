package edu.leipzig.grafs.operators;

import edu.leipzig.grafs.model.Edge;
import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.model.Vertex;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.util.Collector;

public class DummyCollector<V extends Vertex, E extends Edge> implements Collector<Triplet<V, E>> {

  private final List<Triplet<V, E>> collected;

  public DummyCollector() {
    collected = new ArrayList<>();
  }

  public List<Triplet<V, E>> getCollected() {
    return collected;
  }


  @Override
  public void collect(Triplet<V, E> triplet) {
    collected.add(triplet);
  }

  @Override
  public void close() {

  }
}
