package edu.leipzig.grafs.setup.test;

import edu.leipzig.grafs.model.Triplet;
import java.util.LinkedList;
import java.util.List;

public class SmallQueue {

  private final LinkedList<Triplet> queue;

  public SmallQueue() {
    queue = new LinkedList<>();
  }

  public void add(Triplet triplet) {
    if (queue.size() >= 10) {
      queue.removeFirst();
    }
    queue.add(triplet);
  }

  public List<Triplet> asColl() {
    return queue;
  }
}
