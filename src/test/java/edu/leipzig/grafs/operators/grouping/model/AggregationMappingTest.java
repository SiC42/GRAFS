package edu.leipzig.grafs.operators.grouping.model;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import edu.leipzig.grafs.util.TestUtils;
import java.util.Optional;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.junit.jupiter.api.Test;

public class AggregationMappingTest {


  @Test
  public void testAddAggregationForProperty() {
    var am = new AggregationMapping();
    var paf = TestUtils.INT_ADD_FUNC;
    var key = "1";
    am.addAggregationForProperty(key, paf);
    assertThat(am.getAggregationForProperty(key), is(equalTo(paf)));
  }

  @Test
  public void testContainsAggregationForProperty() {
    var am = new AggregationMapping();
    var paf = TestUtils.INT_ADD_FUNC;
    var key = "1";
    am.addAggregationForProperty(key, paf);
    assertThat(am.containsAggregationForProperty(key), is(true));
  }

  @Test
  public void testGetAggregationForMembership_empty() {
    var am = new AggregationMapping();
    assertThat(am.getAggregationForMembership(), is(equalTo(Optional.empty())));
  }

  @Test
  public void testGetAggregationForMembership_notEmpty() {
    var am = new AggregationMapping();
    SerializableBiFunction<GradoopIdSet, GradoopIdSet, GradoopIdSet> membershipAggregation =
        (id1, id2) -> id1;
    am.setAggregationForMembership(membershipAggregation);
    assertThat(am.getAggregationForMembership().isPresent(), is(true));
    assertThat(am.getAggregationForMembership().get(), is(equalTo(membershipAggregation)));

  }

  @Test
  public void testEntrySet() {
    var am = new AggregationMapping();
    var key1 = "1";
    var paf1 = TestUtils.INT_ADD_FUNC;
    var key2 = "2";
    var paf2 = TestUtils.STRING_CONC_FUNC;
    am.addAggregationForProperty(key1, paf1);
    am.addAggregationForProperty(key2, paf2);
    assertThat(am.entrySet(), hasSize(2));
    for (var entry : am.entrySet()) {
      if (entry.getPropertyKey().equalsIgnoreCase(key1)) {
        assertThat(entry.getAggregationFunction(), is(equalTo(paf1)));
      } else {
        assertThat(entry.getAggregationFunction(), is(equalTo(paf2)));
      }
    }

  }
}