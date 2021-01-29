package edu.leipzig.grafs.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import java.util.Collections;
import java.util.Set;
import org.junit.jupiter.api.Test;

class MultiMapTest {

  @Test
  void testAsMap() {
    var multiMap = new MultiMap<Integer, Integer>();
    multiMap.put(1, 2);
    multiMap.put(1, 3);
    multiMap.put(4, 2);
    multiMap.put(4, 6);
    var map = multiMap.asMap();
    assertThat(map, aMapWithSize(2));
    assertThat(map.get(1), is(equalTo(Set.of(2, 3))));
    assertThat(map.get(4), is(equalTo(Set.of(2, 6))));
  }

  @Test
  void testClear() {
    var multiMap = new MultiMap<Integer, Integer>();
    multiMap.put(1, 2);
    assertThat(multiMap.isEmpty(), is(false));
    multiMap.clear();
    assertThat(multiMap.isEmpty(), is(true));
    assertThat(multiMap.size(), is(0));
  }

  @Test
  void testContainsEntry() {
    var multiMap = new MultiMap<Integer, Integer>();
    multiMap.put(1, 2);
    multiMap.put(1, 3);
    multiMap.put(4, 2);
    multiMap.put(4, 6);

    assertThat(multiMap.containsEntry(1, 2), is(true));
    assertThat(multiMap.containsEntry(1, 3), is(true));
    assertThat(multiMap.containsEntry(4, 2), is(true));
    assertThat(multiMap.containsEntry(4, 6), is(true));

    assertThat(multiMap.containsEntry(1, 6), is(false));
    assertThat(multiMap.containsEntry(4, 3), is(false));

    assertThat(multiMap.containsEntry(-1, 3), is(false));
  }

  @Test
  void testContainsKey() {
    var multiMap = new MultiMap<Integer, Integer>();
    multiMap.put(1, 2);
    multiMap.put(1, 3);
    multiMap.put(4, 2);
    multiMap.put(4, 6);

    assertThat(multiMap.containsKey(1), is(true));
    assertThat(multiMap.containsKey(4), is(true));

    assertThat(multiMap.containsKey(2), is(false));
    assertThat(multiMap.containsKey(3), is(false));
    assertThat(multiMap.containsKey(6), is(false));
  }

  @Test
  void testContainsValue() {
    var multiMap = new MultiMap<Integer, Integer>();
    multiMap.put(1, 2);
    multiMap.put(1, 3);
    multiMap.put(4, 6);
    multiMap.put(7, 1);

    assertThat(multiMap.containsValue(4), is(false));
    assertThat(multiMap.containsValue(7), is(false));

    assertThat(multiMap.containsValue(1), is(true));
    assertThat(multiMap.containsValue(2), is(true));
    assertThat(multiMap.containsValue(3), is(true));
    assertThat(multiMap.containsValue(6), is(true));
  }

  @Test
  void testGet() {
    var multiMap = new MultiMap<Integer, Integer>();
    multiMap.put(1, 2);
    multiMap.put(1, 3);
    multiMap.put(4, 2);
    multiMap.put(4, 6);

    assertThat(multiMap.get(1), is(equalTo(Set.of(2, 3))));
    assertThat(multiMap.get(4), is(equalTo(Set.of(2, 6))));
  }

  @Test
  void testGet_emptyValueSet() {
    var multiMap = new MultiMap<Integer, Integer>();

    assertThat(multiMap.get(1), is(equalTo(Collections.EMPTY_SET)));
  }

  @Test
  void testIsEmpty() {
    var multiMap = new MultiMap<Integer, Integer>();
    assertThat(multiMap.isEmpty(), is(true));
    multiMap.put(1, 2);
    assertThat(multiMap.isEmpty(), is(false));
    multiMap.remove(1, 2);
    assertThat(multiMap.isEmpty(), is(true));
  }

  @Test
  void testKeySet() {
    var multiMap = new MultiMap<Integer, Integer>();
    multiMap.put(1, 2);
    multiMap.put(1, 3);
    multiMap.put(4, 2);
    multiMap.put(4, 6);

    assertThat(multiMap.keySet(), is(equalTo(Set.of(1, 4))));
  }


  @Test
  void testPutAll() {
    var multiMap = new MultiMap<Integer, Integer>();
    multiMap.putAll(1, Set.of(1, 2));
    assertThat(multiMap.get(1), is(equalTo(Set.of(1, 2))));
    assertThat(multiMap.size(), is(2));
    multiMap.putAll(1, Set.of(1, 3));
    assertThat(multiMap.get(1), is(equalTo(Set.of(1, 2, 3))));
    assertThat(multiMap.size(), is(3));
  }

  @Test
  void testRemove() {
    var multiMap = new MultiMap<Integer, Integer>();
    multiMap.putAll(1, Set.of(1, 2));
    assertThat(multiMap.get(1), is(equalTo(Set.of(1, 2))));
    assertThat(multiMap.size(), is(2));
    multiMap.remove(1, 2);
    assertThat(multiMap.get(1), is(equalTo(Set.of(1))));
    assertThat(multiMap.size(), is(1));
  }

  @Test
  void testRemoveAll_withValues() {
    var multiMap = new MultiMap<Integer, Integer>();
    multiMap.putAll(1, Set.of(1, 2, 3));
    assertThat(multiMap.get(1), is(equalTo(Set.of(1, 2, 3))));
    assertThat(multiMap.size(), is(3));
    multiMap.removeAll(1, Set.of(2, 3));
    assertThat(multiMap.get(1), is(equalTo(Set.of(1))));
    assertThat(multiMap.size(), is(1));
  }

  @Test
  void testRemoveAll_withKey() {
    var multiMap = new MultiMap<Integer, Integer>();
    multiMap.putAll(1, Set.of(1, 2, 3));
    multiMap.putAll(4, Set.of(5, 6, 7));
    assertThat(multiMap.size(), is(6));
    multiMap.removeAll(1);
    assertThat(multiMap.get(4), is(equalTo(Set.of(5, 6, 7))));
    assertThat(multiMap.get(1), is(empty()));
    assertThat(multiMap.size(), is(3));
  }

  @Test
  void testRetainAll() {
    var multiMap = new MultiMap<Integer, Integer>();
    multiMap.putAll(1, Set.of(1, 2, 3));
    multiMap.retainAll(1, Set.of(2, 4));
    assertThat(multiMap.get(1), is(equalTo(Set.of(2))));
  }

  @Test
  void testValues() {
    var multiMap = new MultiMap<Integer, Integer>();
    multiMap.putAll(1, Set.of(1, 2, 3));
    multiMap.putAll(4, Set.of(3, 4, 5, 6, 7));
    assertThat(multiMap.values(), containsInAnyOrder(1, 2, 3, 3, 4, 5, 6, 7));
  }
}