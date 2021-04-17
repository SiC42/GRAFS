package edu.leipzig.grafs.operators.grouping.functions;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import edu.leipzig.grafs.model.Element;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.junit.jupiter.api.Test;

class MinPropertyTest {

  @Test
  public void testAggregate() {
    final var agg_key = "min";
    var minFunc = new MinProperty(agg_key);
    for (var i = 0; i < 1000; i++) {
      for (var j = 0; i < 1000; i++) {
        var min = Math.min(i, j);
        var pv1 = PropertyValue.create(i);
        var pv2 = PropertyValue.create(j);
        assertThat(minFunc.aggregate(pv1, pv2), is(equalTo(PropertyValue.create(min))));
      }
    }
  }

  @Test
  public void testGetIncrement() {
    final var agg_key = "min";
    var min = new MinProperty(agg_key);

    var element = mock(Element.class);
    var expectedVal = 5L;
    when(element.getPropertyValue(agg_key)).thenReturn(PropertyValue.create(expectedVal));
    assertThat(min.getIncrement(element), is(equalTo(PropertyValue.create(expectedVal))));
  }

}