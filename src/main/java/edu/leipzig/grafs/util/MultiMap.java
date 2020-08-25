package edu.leipzig.grafs.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
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
    return size == 0;
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

  public void removeAll(K key) {
    var valueSet = map.get(key);
    if (valueSet != null) {
      size -= valueSet.size();
      valueSet.clear();
    }
    map.put(key, valueSet);
  }

  public int size() {
    return size;
  }

  @Override
  public String toString() {
    return map.toString();
  }

  public boolean retainAll(K key, Collection<V> retainCollection) {
    var valueSet = map.get(key);
    return valueSet.retainAll(retainCollection);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MultiMap<?, ?> multiMap = (MultiMap<?, ?>) o;
    return size == multiMap.size &&
        Objects.equals(map, multiMap.map);
  }

  @Override
  public int hashCode() {
    return Objects.hash(map, size);
  }

  public Collection<V> values() {
    var values = new ArrayList<V>(size);
    for (var valuesPerKey : map.values()) {
      values.addAll(valuesPerKey);
    }
    return values;
  }
}
