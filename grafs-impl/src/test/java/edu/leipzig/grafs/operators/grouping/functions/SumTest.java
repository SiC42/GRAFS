package edu.leipzig.grafs.operators.grouping.functions;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.Is.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;

import org.gradoop.common.model.impl.properties.PropertyValue;
import org.junit.jupiter.api.Test;

public class SumTest {


  @Test
  public void testAggregate() {
    var sumFunc = mock(Sum.class);
    doCallRealMethod().when(sumFunc).aggregate(any(PropertyValue.class), any(PropertyValue.class));
    for (var i = 0; i < 1000; i++) {
      for (var j = 0; i < 1000; i++) {
        var sum = i + j;
        var pv1 = PropertyValue.create(i);
        var pv2 = PropertyValue.create(j);

        assertThat(sumFunc.aggregate(pv1, pv2), is(equalTo(PropertyValue.create(sum))));
      }
    }
  }

}
