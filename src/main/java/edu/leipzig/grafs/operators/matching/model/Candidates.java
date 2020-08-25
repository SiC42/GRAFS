package edu.leipzig.grafs.operators.matching.model;

import edu.leipzig.grafs.util.MultiMap;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;

public class Candidates<E> {

  private final MultiMap<E, E> candidateMap;

  public Candidates() {
    candidateMap = new MultiMap<>();
  }

  public boolean addCandidate(E keyElem, E candidate) {
    return candidateMap.put(keyElem, candidate);
  }

  public boolean hasCandidateFor(E keyElem) {
    return !candidateMap.get(keyElem).isEmpty();
  }

  public Set<E> getCandidatesFor(E keyElem) {
    return candidateMap.get(keyElem);
  }

  public boolean hasCandidate(E keyElem, E potentialCandidate) {
    return candidateMap.get(keyElem).contains(potentialCandidate);
  }

  public void removeCandidates(E keyElem, Collection<E> candidatesToDelete) {
    candidateMap.removeAll(keyElem, candidatesToDelete);
  }

  public void removeCandidates(E key) {
    candidateMap.removeAll(key);
  }

  public void removeCandidate(E keyElem, E candidateToDelete) {
    candidateMap.remove(keyElem, candidateToDelete);
  }

  public void addCandidates(E keyElem, Set<E> newCandidates) {
    candidateMap.putAll(keyElem, newCandidates);
  }

  public void addCandidates(Candidates<E> candidates) {
    for (var keyElem : candidates.candidateMap.keySet()) {
      addCandidates(keyElem, candidates.getCandidatesFor(keyElem));
    }
  }

  public MultiMap<E, E> asMultiMap() {
    return candidateMap;
  }

  public boolean isEmpty() {
    return candidateMap.isEmpty();
  }

  public int size() {
    return candidateMap.size();
  }

  @Override
  public String toString() {
    return "Candidates{" + candidateMap + '}';
  }

  public boolean retainCandidates(E keyElem, Collection<E> retainCollection) {
    return candidateMap.retainAll(keyElem, retainCollection);
  }

  public Set<E> keySet() {
    return candidateMap.keySet();
  }

  public boolean isCandidate(E elem) {
    for (var value : candidateMap.values()) {
      if (value.equals(elem)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Candidates<?> that = (Candidates<?>) o;
    return Objects.equals(candidateMap, that.candidateMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(candidateMap);
  }
}