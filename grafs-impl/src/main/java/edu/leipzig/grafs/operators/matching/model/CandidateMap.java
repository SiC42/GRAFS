package edu.leipzig.grafs.operators.matching.model;

import edu.leipzig.grafs.util.MultiMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;

/**
 * Represents a candidate map, i.e. for each key-element, the values are candidates of the
 * key-element.
 *
 * @param <E> the type of elements maintained by this candidate map
 */
public class CandidateMap<E> {

  private final MultiMap<E, E> candidateMap;

  /**
   * Constrcuts a new, empty candidate map; the backing HashMap instance has default initial
   * capacity (16) and load factor (0.75).
   */
  public CandidateMap() {
    candidateMap = new MultiMap<>();
  }

  /**
   * Adds an element to the candidate set of the given key element.
   *
   * @param keyElem   key element for which a candidate should be added
   * @param candidate candidate for the given key element that should be added to the map
   * @return <tt>true</tt> if the map has been updated, i.e. the given candidate was not added
   * before for the given key
   */
  public boolean addCandidate(E keyElem, E candidate) {
    return candidateMap.put(keyElem, candidate);
  }

  /**
   * Adds the given elements to the candidate set of the given key element.
   *
   * @param keyElem       key element for which the given elements should be added to the set of
   *                      candidates
   * @param newCandidates elements which should be added as candidates for the given key element
   */
  public void addCandidates(E keyElem, Set<E> newCandidates) {
    candidateMap.putAll(keyElem, newCandidates);
  }

  /**
   * Updates this map by adding all elements of the given candidate map and its candidates to this
   * map.
   *
   * @param candidateMap candidate map which should be added to this map
   */
  public void addCandidates(CandidateMap<E> candidateMap) {
    for (var keyElem : candidateMap.candidateMap.keySet()) {
      addCandidates(keyElem, candidateMap.getCandidatesFor(keyElem));
    }
  }

  /**
   * Returns <tt>true</tt> if the given element has candidates in the map.
   *
   * @param keyElem key element for which the presence of candidates should be tested
   * @return <tt>true</tt> if the given element has candidates in the map
   */
  public boolean hasCandidateFor(E keyElem) {
    return !candidateMap.get(keyElem).isEmpty();
  }

  /**
   * Returns the candidates for the given key element.
   *
   * @param keyElem key element for which the candidates should be returned
   * @return the candidates for the given key element
   */
  public Set<E> getCandidatesFor(E keyElem) {
    return candidateMap.get(keyElem);
  }

  /**
   * Returns <tt>true</tt> if the given element is a candidate for the given key element.
   *
   * @param keyElem            key element for which the presence of the given candidate should be
   *                           tested
   * @param potentialCandidate element for which the it should be tasted if it is a candidate for
   *                           the given key element
   * @return <tt>true</tt> if the given element is a candidate for the given key element
   */
  public boolean hasCandidate(E keyElem, E potentialCandidate) {
    return candidateMap.get(keyElem).contains(potentialCandidate);
  }

  /**
   * Removes all given elements from the candidate set of the given key element.
   *
   * @param keyElem            key element for which the candidate set should be updated by deleting
   *                           the given elements
   * @param candidatesToDelete elements which should be deleted from the candidate set of the given
   *                           key element
   */
  public void removeCandidates(E keyElem, Collection<E> candidatesToDelete) {
    candidateMap.removeAll(keyElem, candidatesToDelete);
  }

  /**
   * Removes all candidates from the candidate set of the given key element.
   *
   * @param keyElem key element for which the candidate set should be deleted
   */
  public void removeCandidates(E keyElem) {
    candidateMap.removeAll(keyElem);
  }

  /**
   * Removes the given element from the candidate set of the given key element.
   *
   * @param keyElem           key element for which the candidate set should be updated by deleting
   *                          the given element
   * @param candidateToDelete element which should be deleted from the candidate set of the given
   *                          key element
   */
  public void removeCandidate(E keyElem, E candidateToDelete) {
    candidateMap.remove(keyElem, candidateToDelete);
  }

  /**
   * Returns this candidate map as {@link MultiMap}.
   *
   * @return {@link MultiMap}-representation of this object
   */
  public MultiMap<E, E> asMultiMap() {
    return candidateMap;
  }

  /**
   * Returns <tt>true</tt> if the candidate map contains no candidates, <tt>false</tt> otherwise.
   *
   * @return <tt>true</tt> if the candidate map contains no candidates, <tt>false</tt> otherwise
   */
  public boolean isEmpty() {
    return candidateMap.isEmpty();
  }

  /**
   * Returns the number of candidates in this map. If a candidate is listed in to different
   * candidate sets, it still counts as two distinc candidates.
   *
   * @return the number of candidates in this map
   */
  public int size() {
    return candidateMap.size();
  }

  /**
   * Returns this map as a list of candidate sets.
   *
   * @return this map as a list of candidate sets
   */
  public ArrayList<Set<E>> asListOfCandidateSets() {
    return new ArrayList<>(candidateMap.asMap().values());
  }

  /**
   * Returns a string representation of this map. Representation is the same as a {@link MultiMap}
   * representation.
   *
   * @return a string representation of this map
   */
  @Override
  public String toString() {
    return "Candidates" + candidateMap;
  }

  /**
   * Retains only the given elements as candidates for the given key element and discards all other
   * candidates.
   *
   * @param keyElem          key element for which the candidate set should be updated
   * @param retainCollection collection of elements which should be retained
   * @return <tt>true</tt> if the map updated, i.e. there where elements deleted from the candidate
   * set of the given key
   */
  public boolean retainCandidates(E keyElem, Collection<E> retainCollection) {
    return candidateMap.retainAll(keyElem, retainCollection);
  }

  /**
   * Returns a {@link Set} view of the keys contained in this map.
   *
   * @return a set view of the keys contained in this map
   */
  public Set<E> keySet() {
    return candidateMap.keySet();
  }

  /**
   * Returns <tt>true</tt> if the given element is in a candidate set for any key element,
   * <tt>false</tt> otherwise.
   *
   * @param elem element for which should be tested if is a candidate of any element in this map
   * @return <tt>true</tt> if the given element is in a candidate set for any key element,
   * <tt>false</tt> otherwise
   */
  public boolean isCandidate(E elem) {
    for (var value : candidateMap.values()) {
      if (value.equals(elem)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Compares the specified object with this map for equality. Returns true if the given object is
   * also a candidate mapo and the two maps represent the same mappings.
   *
   * @param o object to be compared for equality with this map
   * @return <tt>true</tt> if the specified object is equal to this map
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CandidateMap<?> that = (CandidateMap<?>) o;
    return Objects.equals(candidateMap, that.candidateMap);
  }

  /**
   * Returns a hash code for this map.
   *
   * @return the hash code value for this map
   */
  @Override
  public int hashCode() {
    return candidateMap.hashCode();
  }
}