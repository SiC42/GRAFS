package edu.leipzig.grafs.util;

import java.util.HashSet;
import java.util.Set;

public class Sets {

  public static <E> Set<E> intersection(Set<E> set1, Set<E> set2) {
    var intersection = new HashSet<>(set1);
    intersection.retainAll(set2);
    return intersection;
  }

  public static <E> Set<E> union(Set<E> set1, Set<E> set2) {
    var union = new HashSet<>(set1);
    union.addAll(set2);
    return union;
  }

}
