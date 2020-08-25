package edu.leipzig.grafs.operators.matching.model;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import java.util.Set;
import org.junit.jupiter.api.Test;

class CandidatesTest {


  @Test
  void testHasCandidateFor() {
    var candidates = new Candidates<Integer>();
    candidates.addCandidate(1, 2);
    assertThat(candidates.hasCandidateFor(1), is(true));
    assertThat(candidates.hasCandidateFor(2), is(false));
  }

  @Test
  void testGetCandidatesFor() {
    var candidates = new Candidates<Integer>();
    candidates.addCandidate(1, 2);
    candidates.addCandidate(1, 3);
    candidates.addCandidate(4, 2);
    candidates.addCandidate(4, 6);

    assertThat(candidates.getCandidatesFor(1), is(equalTo(Set.of(2, 3))));
    assertThat(candidates.getCandidatesFor(4), is(equalTo(Set.of(2, 6))));
  }

  @Test
  void testHasCandidate() {
  }

  @Test
  void testDeleteCandidates() {
  }

  @Test
  void testDeleteCandidate() {
  }

  @Test
  void testAddCandidates() {
  }

  @Test
  void testTestAddCandidates() {
  }

  @Test
  void testAsMultiMap() {
  }

  @Test
  void testIsEmpty() {
  }

  @Test
  void testSize() {
  }

  @Test
  void testRetainCandidates() {
  }

  @Test
  void testRemoveAll() {
  }

  @Test
  void testKeySet() {
  }

  @Test
  void testIsCandidate() {
  }
}