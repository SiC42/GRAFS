package edu.leipzig.grafs.operators;

import edu.leipzig.grafs.model.EdgeContainer;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.util.Collector;

public class DummyCollector implements Collector<EdgeContainer> {

  private final List<EdgeContainer> collected;

  public DummyCollector() {
    collected = new ArrayList<>();
  }

  public List<EdgeContainer> getCollected() {
    return collected;
  }


  @Override
  public void collect(EdgeContainer edgeContainer) {
    collected.add(edgeContainer);
  }

  @Override
  public void close() {

  }
}
