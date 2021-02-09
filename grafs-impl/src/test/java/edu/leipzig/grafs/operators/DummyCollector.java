package edu.leipzig.grafs.operators;

import edu.leipzig.grafs.model.Edge;
import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.model.Vertex;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.util.Collector;

public class DummyCollector implements Collector<Triplet<Vertex, Edge>> {

  private final List<Triplet<Vertex, Edge>> collected;

  public DummyCollector() {
    collected = new ArrayList<>();
  }

  public List<Triplet<Vertex, Edge>> getCollected() {
    return collected;
  }


  @Override
  public void collect(Triplet<Vertex, Edge> triplet) {
    collected.add(triplet);
  }

  @Override
  public void close() {

  }
}
