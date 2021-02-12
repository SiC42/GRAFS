package edu.leipzig.grafs.util;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * This class represents a multimap, i.e. a map which can hold multiple (distinct) values for each
 * key.
 *
 * @param <K> the type of keys maintained by this map
 * @param <V> the type of mapped values
 */
public class MultiMap<K, V> implements Serializable {

  protected Map<K, Set<V>> map;
  protected int size;

  /**
   * Initializes an empty multi map with the default initial capacity (16) and the default load
   * factor (0.75).
   */
  public MultiMap() {
    map = new HashMap<>();
    size = 0;
  }

  /**
   * Initializes an empty multi map with the default initial capacity (16) and the default load
   * factor (0.75).
   */
  public MultiMap(MultiMap<K,V> otherMultiMap) {
    this();
    for(var key : otherMultiMap.keySet()){
      for(var value : otherMultiMap.get(key)){
        put(key, value);
      }
    }
  }

  /**
   * Returns the map representation of this object, i.e. a map of keys and sets of values.
   *
   * @return the map representation of this object
   */
  public Map<K, Set<V>> asMap() {
    return map;
  }

  /**
   * Removes all of the mappings from this map.
   */
  public void clear() {
    map.clear();
    size = 0;
  }

  /**
   * Returns <tt>true</tt> if this map contains the specified key and the given value is associated
   * with the given key.
   *
   * @param key   key whose presence in this map is to be tested
   * @param value value whose association with the given key is to be tested
   * @return <tt>true</tt> if this map contains the specified key and the given value is associated
   * with the given key
   */
  public boolean containsEntry(K key, V value) {
    if (!map.containsKey(key)) {
      return false;
    }
    var valueSet = map.get(key);
    return valueSet.contains(value);
  }

  /**
   * Returns <tt>true</tt> if this map contains a mapping for the specified key.
   *
   * @param key the key whose presence in this map is to be tested
   * @return <tt>true</tt> if this map contains a mapping for the specified key
   */
  public boolean containsKey(K key) {
    return map.containsKey(key);
  }

  /**
   * Returns <tt>true</tt> if this map maps one ore more keys to the specified value.
   *
   * @param value the value whose presence in this map is to be tested
   * @return <tt>true</tt> if this map maps one or more keys to the specified value
   */
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

  /**
   * Returns the set of values to which the specified key is mapped, or an empty set otherwise.
   *
   * @param key the key whose associated values are to be returned
   * @return the set of values to which the specified key are mapped, or an empty set if this map
   * contains no mapping for the key
   */
  public Set<V> get(K key) {
    var valueSet = map.get(key);
    return valueSet != null ? valueSet : Collections.emptySet();
  }

  /**
   * Returns <tt>true</tt> if the this map contains no values, i.e. if for each key there is no
   * mapping or just an empty set.
   *
   * @return <tt>true</tt> if the this map contains no values
   */
  public boolean isEmpty() {
    return size == 0;
  }

  /**
   * Returns a Set view of the keys contained in this map.
   *
   * @return a set view of the keys contained in this map
   */
  public Set<K> keySet() {
    return map.keySet();
  }

  /**
   * Associates the specified value with the specified key in this map.
   *
   * @param key   key with which the specified value is to be associated
   * @param value value to be associated with the specified key
   * @return <tt>true</tt> if the map has changed, i.e. if the given key-value pair was not
   * associated before
   */
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

  /**
   * Associates all given values with the specified key in this map.
   *
   * @param key    key with which the given values are to be associated
   * @param values collection of values to be associated with the specified key
   * @return <tt>true</tt> if the map has changed, i.e. if there is at least one key-value pair that
   * was not associated before
   */
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

  /**
   * Removes the mapping for the specified key and value from this map if present.
   *
   * @param key   key whose value mapping is to be removed from the map
   * @param value value whose association should be removed from the given key
   * @return <tt>true</tt> if the map has changed, i.e. if the given key-value pair was associated
   * before
   */
  public boolean remove(K key, V value) {
    var valueSet = map.get(key);
    if (valueSet == null) {
      return false;
    }
    boolean hasChanged = valueSet.remove(value);
    if (hasChanged) {
      size--;
      map.put(key, valueSet);
    }
    return hasChanged;
  }

  /**
   * Removes the mapping for the specified key and the values from this map if present.
   *
   * @param key    key whose value mapping is to be removed from the map
   * @param values values whose association should be removed from the given key
   * @return <tt>true</tt> if the map has changed, i.e. if there is at least one key-value pair that
   * was associated before
   */
  public boolean removeAll(K key, Collection<V> values) {
    var valueSet = map.get(key);
    if (valueSet == null) {
      return false;
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

  /**
   * Removes the mapping for the specified key from this map if present.
   *
   * @param key key whose mapping is to be removed from the map
   */
  public void removeAll(K key) {
    var valueSet = map.get(key);
    if (valueSet != null) {
      size -= valueSet.size();
    }
    map.remove(key);
  }

  /**
   * Returns the number of key-value mappings.
   *
   * @return the number of key-value mappings
   */
  public int size() {
    return size;
  }

  /**
   * Returns string representation of this map.
   *
   * @return string representation of this map
   */
  @Override
  public String toString() {
    return map.toString();
  }

  /**
   * Retains all given values for the given key. Other values are deleted from the association of
   * the given key
   *
   * @param key              key for which the values should be retained
   * @param retainCollection values which should be retained for the given key
   * @return <tt>true</tt> if this map changed as a result of this call
   */
  public boolean retainAll(K key, Collection<V> retainCollection) {
    var valueSet = map.get(key);
    return valueSet.retainAll(retainCollection);
  }

  /**
   * Compares the specified object with this multi map for equality. Returns <tt>true</tt>> if the
   * specified object is also a multi set, the two multi maps have the same size, and the underlying
   * maps are the same.
   *
   * @param o object to be compared for equality with this multi map
   * @return <tt>true</tt> if the specified object is equal to this multi map
   */
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

  /**
   * Returns the hash code value for this multi map.
   *
   * @return the hash code value for this multi map
   */
  @Override
  public int hashCode() {
    return Objects.hash(map, size);
  }

  /**
   * Returns all values in this map. There may be duplicates of values in the collection.
   *
   * @return all values in this map
   */
  public Collection<V> values() {
    var values = new ArrayList<V>(size);
    for (var valuesPerKey : map.values()) {
      values.addAll(valuesPerKey);
    }
    return values;
  }

  protected void writeObject(java.io.ObjectOutputStream out)
      throws IOException {
    out.writeInt(map.keySet().size());
    for (var key : map.keySet()) {
      out.writeObject(key);
      var values = map.get(key);
      out.writeInt(values.size());
      for(var value : values){
        out.writeObject(value);
      }
    }
  }

  protected void readObject(java.io.ObjectInputStream in)
      throws IOException, ClassNotFoundException {
    var keySetSize = in.readInt();
    map = new HashMap<>();
    for(int i = 0; i< keySetSize; i++){
      var key = (K) in.readObject();
      var valueSetSize = in.readInt();
      for(int j = 0; j< valueSetSize; j++){
        var value = (V) in.readObject();
        put(key, value);
      }
    }
  }
}
