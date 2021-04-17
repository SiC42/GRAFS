package edu.leipzig.grafs.operators.grouping.functions;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.Is.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;

import org.junit.jupiter.api.Test;

public class BaseAggregateFunctionTest {

  private static final String AGGREGATE_DEFAULT_KEY = "aggregate";

  @Test
  public void testDefaultConstructor() {
    var settings = withSettings().useConstructor();
    var aggFunc = mock(BaseAggregateFunction.class, settings);
    doCallRealMethod().when(aggFunc).getAggregatePropertyKey();
    assertThat(aggFunc.getAggregatePropertyKey(), is(equalTo(AGGREGATE_DEFAULT_KEY)));
  }

  @Test
  public void testConstructorWithKey() {
    final var expectedString = "test";
    var settings = withSettings().useConstructor(expectedString);
    var aggFunc = mock(BaseAggregateFunction.class, settings);
    doCallRealMethod().when(aggFunc).getAggregatePropertyKey();
    assertThat(aggFunc.getAggregatePropertyKey(), is(equalTo(expectedString)));
  }

  @Test
  public void testSetAggregatePropertyKey() {
    var settings = withSettings().useConstructor();
    var aggFunc = mock(BaseAggregateFunction.class, settings);
    doCallRealMethod().when(aggFunc).getAggregatePropertyKey();
    doCallRealMethod().when(aggFunc).setAggregatePropertyKey(any(String.class));
    assertThat(aggFunc.getAggregatePropertyKey(), is(equalTo(AGGREGATE_DEFAULT_KEY)));
    final var expectedNewString = "test";
    aggFunc.setAggregatePropertyKey(expectedNewString);
    assertThat(aggFunc.getAggregatePropertyKey(), is(equalTo(expectedNewString)));
  }

}
