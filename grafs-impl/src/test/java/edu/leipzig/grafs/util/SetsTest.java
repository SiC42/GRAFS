package edu.leipzig.grafs.util;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Set;
import org.junit.jupiter.api.Test;

class SetsTest {

  @Test
  void testIntersection() {
    var set1 = Set.of(1, 2, 3, 4, 5);
    var set2 = Set.of(1, 3, 4, 6);
    assertThat(Sets.intersection(set1, set2), equalTo(Set.of(1, 3, 4)));
  }

  @Test
  void testUnion() {
    var set1 = Set.of(1, 2, 3, 4, 5);
    var set2 = Set.of(1, 3, 4, 6);
    assertThat(Sets.union(set1, set2), equalTo(Set.of(1, 2, 3, 4, 5, 6)));
  }
}