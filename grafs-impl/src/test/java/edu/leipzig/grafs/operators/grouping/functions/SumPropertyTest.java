package edu.leipzig.grafs.operators.grouping.functions;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import edu.leipzig.grafs.model.Element;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.junit.jupiter.api.Test;

class SumPropertyTest {

  @Test
  public void testGetIncrement() {
    final var agg_key = "max";
    var max = new SumProperty(agg_key);

    var element = mock(Element.class);
    var expectedVal = 5L;
    when(element.getPropertyValue(agg_key)).thenReturn(PropertyValue.create(expectedVal));
    assertThat(max.getIncrement(element), is(equalTo(PropertyValue.create(expectedVal))));
  }

}