package edu.leipzig.grafs.operators.grouping.model;


import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import edu.leipzig.grafs.util.TestUtils;
import java.io.IOException;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.junit.jupiter.api.Test;

public class PropertiesAggregationFunctionTest {

  @Test
  public void testSerialization() throws IOException, ClassNotFoundException {
    var pv = new PropertyValue();
    pv.setString(TestUtils.STRING_VAL_6);
    var concPropVal = TestUtils.STRING_CONC_FUNC
        .apply(PropertyValue.create("1"), PropertyValue.create("2"));
    byte[] serializedPaf = TestUtils.pickle(TestUtils.STRING_CONC_FUNC);
    var deserializedPaf = TestUtils.unpickle(serializedPaf, PropertiesAggregationFunction.class);
    var pvFromDeserializedPaf = deserializedPaf
        .apply(PropertyValue.create("1"), PropertyValue.create("2"));
    assertThat(pvFromDeserializedPaf, is(equalTo(concPropVal)));
    assertThat(deserializedPaf.getIdentity(),
        is(equalTo(TestUtils.STRING_CONC_FUNC.getIdentity())));
  }

}