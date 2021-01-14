package edu.leipzig.grafs.operators;

import edu.leipzig.grafs.model.Triplet;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.util.Collector;

public class DummyCollector implements Collector<Triplet> {

  private final List<Triplet> collected;

  public DummyCollector() {
    collected = new ArrayList<>();
  }

  public List<Triplet> getCollected() {
    return collected;
  }


  @Override
  public void collect(Triplet triplet) {
    collected.add(triplet);
  }

  @Override
  public void close() {

  }
}
