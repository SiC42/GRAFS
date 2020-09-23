package edu.leipzig.grafs.operators.grouping.model;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import edu.leipzig.grafs.util.TestUtils;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.junit.jupiter.api.Test;

public class AggregationMappingEntryTest {

  @Test
  public void AggregationMappingEntry() {
    var pv = new PropertyValue();
    pv.setString(TestUtils.STRING_VAL_6);
    var paf = TestUtils.INT_ADD_FUNC.apply(TestUtils.KEY_0);
    var ame = new AggregationMappingEntry(TestUtils.KEY_0, paf);
    assertThat(ame.getPropertyKey(), is(equalTo(TestUtils.KEY_0)));
    assertThat(ame.getAggregationFunction(), is(equalTo(paf)));

  }
}