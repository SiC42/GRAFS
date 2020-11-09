package edu.leipzig.grafs.setup.test;

import edu.leipzig.grafs.model.EdgeContainer;
import java.util.LinkedList;
import java.util.List;

public class SmallQueue {

  private LinkedList<EdgeContainer> queue;

  public SmallQueue() {
    queue = new LinkedList<>();
  }

  public void add(EdgeContainer ec) {
    if (queue.size() >= 10) {
      queue.removeFirst();
    }
    queue.add(ec);
  }

  public List<EdgeContainer> asColl() {
    return queue;
  }
}
