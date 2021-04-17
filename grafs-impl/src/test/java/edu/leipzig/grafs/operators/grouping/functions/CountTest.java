package edu.leipzig.grafs.operators.grouping.functions;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;

import edu.leipzig.grafs.model.Element;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.junit.jupiter.api.Test;

class CountTest {

  @Test
  public void testGetDefaultValue() {
    final var agg_key = "count";
    var count = new Count(agg_key);

    assertThat(count.getDefaultValue(), is(equalTo(PropertyValue.create(0L))));
  }

  @Test
  public void testGetIncrement() {
    final var agg_key = "count";
    var count = new Count(agg_key);

    var element = mock(Element.class);
    assertThat(count.getIncrement(element), is(equalTo(PropertyValue.create(1L))));
  }

}