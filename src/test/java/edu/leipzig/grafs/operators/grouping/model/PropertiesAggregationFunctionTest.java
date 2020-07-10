package edu.leipzig.grafs.operators.grouping.model;


import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import edu.leipzig.grafs.util.TestUtils;
import java.io.IOException;
import java.util.Random;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.junit.jupiter.api.Test;

public class PropertiesAggregationFunctionTest {

  @Test
  public void testSerialization() throws IOException, ClassNotFoundException {
    var propAggFunc = TestUtils.INT_ADD_FUNC;
    byte[] serializedPaf = TestUtils.pickle(propAggFunc);
    var deserializedPropAggFunc = TestUtils
        .unpickle(serializedPaf, PropertiesAggregationFunction.class);

    assertThat(deserializedPropAggFunc.getIdentity(), is(equalTo(propAggFunc.getIdentity())));

    // Test multiple values, as we can't test for equality
    var random = new Random();
    for (int i = 0; i < 1000; i++) {
      var randomInt1 = random.nextInt(Integer.MAX_VALUE / 2);
      var randomInt2 = random.nextInt(Integer.MAX_VALUE / 2);
      var expectedResult = propAggFunc
          .apply(PropertyValue.create(randomInt1), PropertyValue.create(randomInt2));
      var actualResult = deserializedPropAggFunc
          .apply(PropertyValue.create(randomInt1), PropertyValue.create(randomInt2));
      assertThat(actualResult, is(equalTo(expectedResult)));
    }
  }

}