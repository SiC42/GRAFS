package edu.leipzig.grafs.operators.grouping.functions;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;

import org.gradoop.common.model.impl.properties.PropertyValue;
import org.junit.jupiter.api.Test;

class AggregateFunctionTest {


  @Test
  public void testGetDefault() {
    var aggFunc = mock(AggregateFunction.class);
    doCallRealMethod().when(aggFunc).getDefaultValue();
    assertThat(aggFunc.getDefaultValue(), is(equalTo(PropertyValue.NULL_VALUE)));
  }

}