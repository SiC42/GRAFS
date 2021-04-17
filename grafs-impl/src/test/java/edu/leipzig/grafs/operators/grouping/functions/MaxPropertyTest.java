package edu.leipzig.grafs.operators.grouping.functions;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import edu.leipzig.grafs.model.Element;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.junit.jupiter.api.Test;

class MaxPropertyTest {

  @Test
  public void testAggregate() {
    final var agg_key = "max";
    var maxFunc = new MaxProperty(agg_key);
    for (var i = 0; i < 1000; i++) {
      for (var j = 0; i < 1000; i++) {
        var max = Math.max(i, j);
        var pv1 = PropertyValue.create(i);
        var pv2 = PropertyValue.create(j);
        assertThat(maxFunc.aggregate(pv1, pv2), is(equalTo(PropertyValue.create(max))));
      }
    }
  }

  @Test
  public void testGetIncrement() {
    final var agg_key = "max";
    var max = new MaxProperty(agg_key);

    var element = mock(Element.class);
    var expectedVal = 5L;
    when(element.getPropertyValue(agg_key)).thenReturn(PropertyValue.create(expectedVal));
    assertThat(max.getIncrement(element), is(equalTo(PropertyValue.create(expectedVal))));
  }

}