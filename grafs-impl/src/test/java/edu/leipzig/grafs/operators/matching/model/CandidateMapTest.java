package edu.leipzig.grafs.operators.matching.model;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import java.util.Collections;
import java.util.Set;
import org.junit.jupiter.api.Test;

class CandidateMapTest {


  @Test
  void testHasCandidateFor() {
    var candidates = new CandidateMap<Integer>();
    candidates.addCandidate(1, 2);
    assertThat(candidates.hasCandidateFor(1), is(true));
    assertThat(candidates.hasCandidateFor(2), is(false));
  }

  @Test
  void testGetCandidatesFor() {
    var candidates = new CandidateMap<Integer>();
    candidates.addCandidate(1, 2);
    candidates.addCandidate(1, 3);
    candidates.addCandidate(4, 2);
    candidates.addCandidate(4, 6);

    assertThat(candidates.getCandidatesFor(1), is(equalTo(Set.of(2, 3))));
    assertThat(candidates.getCandidatesFor(4), is(equalTo(Set.of(2, 6))));
  }

  @Test
  void testHasCandidate() {
    var candidates = new CandidateMap<Integer>();
    candidates.addCandidate(1, 2);
    candidates.addCandidate(1, 3);
    candidates.addCandidate(4, 2);
    candidates.addCandidate(4, 6);
    assertThat(candidates.hasCandidate(1, 2), is(true));
    assertThat(candidates.hasCandidate(1, 3), is(true));
    assertThat(candidates.hasCandidate(4, 2), is(true));
    assertThat(candidates.hasCandidate(4, 6), is(true));
  }

  @Test
  void testDeleteCandidates() {
    var candidates = new CandidateMap<Integer>();
    candidates.addCandidates(1, Set.of(1, 2, 3));
    assertThat(candidates.getCandidatesFor(1), is(equalTo(Set.of(1, 2, 3))));
    assertThat(candidates.size(), is(3));
    candidates.removeCandidates(1, Set.of(2, 3));
    assertThat(candidates.getCandidatesFor(1), is(equalTo(Set.of(1))));
    assertThat(candidates.size(), is(1));
  }

  @Test
  void testDeleteCandidate() {
    var candidates = new CandidateMap<Integer>();
    candidates.addCandidates(1, Set.of(1, 2));
    assertThat(candidates.getCandidatesFor(1), is(equalTo(Set.of(1, 2))));
    assertThat(candidates.size(), is(2));
    candidates.removeCandidate(1, 2);
    assertThat(candidates.getCandidatesFor(1), is(equalTo(Set.of(1))));
    assertThat(candidates.size(), is(1));
  }

  @Test
  void testAddCandidates_withCandidateSet() {
    var candidates = new CandidateMap<Integer>();
    candidates.addCandidates(1, Set.of(1, 2));
    assertThat(candidates.getCandidatesFor(1), is(equalTo(Set.of(1, 2))));
    assertThat(candidates.size(), is(2));
    candidates.addCandidates(1, Set.of(1, 3));
    assertThat(candidates.getCandidatesFor(1), is(equalTo(Set.of(1, 2, 3))));
    assertThat(candidates.size(), is(3));
  }

  @Test
  void testAddCandidates_withCandidatesObject() {
    var candidates = new CandidateMap<Integer>();
    candidates.addCandidates(1, Set.of(1, 2));

    var candidates2 = new CandidateMap<Integer>();
    candidates2.addCandidates(1, Set.of(1, 3));
    candidates.addCandidates(candidates2);
    assertThat(candidates.getCandidatesFor(1), is(equalTo(Set.of(1, 2, 3))));
    assertThat(candidates.size(), is(3));
  }

  @Test
  void testAddCandidates_withKeyOnly() {
    var candidates = new CandidateMap<Integer>();
    candidates.addCandidates(1, Set.of(1, 2, 3));
    candidates.addCandidates(4, Set.of(5, 6, 7));
    assertThat(candidates.size(), is(6));
    candidates.removeCandidates(1);
    assertThat(candidates.getCandidatesFor(4), is(equalTo(Set.of(5, 6, 7))));
    assertThat(candidates.getCandidatesFor(1), is(equalTo(Collections.EMPTY_SET)));
    assertThat(candidates.size(), is(3));
  }


  @Test
  void testRetainCandidates() {
    var candidates = new CandidateMap<Integer>();
    candidates.addCandidates(1, Set.of(1, 2, 3));
    candidates.retainCandidates(1, Set.of(2, 4));
    assertThat(candidates.getCandidatesFor(1), is(equalTo(Set.of(2))));
  }

  @Test
  void testIsCandidate() {
    var candidates = new CandidateMap<Integer>();
    candidates.addCandidates(4, Set.of(1, 2, 3));
    candidates.addCandidates(5, Set.of(6, 7, 3));
    for (var i : Set.of(1, 2, 3, 6, 7)) {
      assertThat(candidates.isCandidate(i), is(true));
    }
    assertThat(candidates.isCandidate(4), is(false));
  }
}