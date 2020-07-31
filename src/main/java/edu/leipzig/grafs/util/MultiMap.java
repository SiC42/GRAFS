package edu.leipzig.grafs.util;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class MultiMap<K, V> {

  private final Map<K, Set<V>> map;
  private int size;

  public MultiMap() {
    map = new HashMap<>();
    size = 0;
  }

  public Map<K, Set<V>> asMap() {
    return map;
  }

  public void clear() {
    map.clear();
  }

  public boolean containsEntry(K key, V value) {
    if (!map.containsKey(key)) {
      return false;
    }
    var valueSet = map.get(key);
    return valueSet.contains(value);
  }

  public boolean containsKey(K key) {
    return map.containsKey(key);
  }

  public boolean containsValue(V value) {
    var valuesIterator = map.values().iterator();
    Collection<V> collection;
    do {
      if (!valuesIterator.hasNext()) {
        return false;
      }
      collection = valuesIterator.next();
    } while (!collection.contains(value));
    return true;
  }

  public Set<V> get(K key) {
    return map.get(key);
  }

  public boolean isEmpty() {
    return map.isEmpty();
  }

  public Set<K> keySet() {
    return map.keySet();
  }

  public boolean put(K key, V value) {
    var valueSet = map.get(key);
    if (valueSet == null) {
      valueSet = new HashSet<>();
    }
    boolean hasChanged = valueSet.add(value);
    if (hasChanged) {
      size++;
      map.put(key, valueSet);
    }
    return hasChanged;
  }

  public boolean putAll(K key, Collection<V> values) {
    var valueSet = map.get(key);
    if (valueSet == null) {
      valueSet = new HashSet<>();
    }
    var hasChanged = false;
    for (var value : values) {
      var hasAdded = valueSet.add(value);
      hasChanged = hasChanged || hasAdded;
      if (hasAdded) {
        size++;
      }
    }
    map.put(key, valueSet);
    return hasChanged;
  }

  public boolean remove(K key, V value) {
    var valueSet = map.get(key);
    if (valueSet == null) {
      valueSet = new HashSet<>();
    }
    boolean hasChanged = valueSet.remove(value);
    if (hasChanged) {
      size++;
      map.put(key, valueSet);
    }
    return hasChanged;
  }

  public boolean removeAll(K key, Collection<V> values) {
    var valueSet = map.get(key);
    if (valueSet == null) {
      valueSet = new HashSet<>();
    }
    var hasChanged = false;
    for (var value : values) {
      var hasRemoved = valueSet.remove(value);
      hasChanged = hasChanged || hasRemoved;
      if (hasRemoved) {
        size--;
      }
    }
    map.put(key, valueSet);
    return hasChanged;
  }

  public int size() {
    return size;
  }


}
