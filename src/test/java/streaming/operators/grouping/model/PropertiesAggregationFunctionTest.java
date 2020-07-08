package streaming.operators.grouping.model;


import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import java.io.IOException;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.junit.jupiter.api.Test;
import streaming.util.TestUtils;

public class PropertiesAggregationFunctionTest {

  @Test
  public void testSerialization() throws IOException, ClassNotFoundException {
    var pv = new PropertyValue();
    pv.setString(TestUtils.STRING_VAL_6);
    var paf = new PropertiesAggregationFunction(pv, (v1, v2) -> PropertyValue.create(v1.getString() + v2.getString()));
    var pvFromPaf = paf.apply(PropertyValue.create("1"), PropertyValue.create("2"));
    byte[] serializedPaf = TestUtils.pickle(paf);
    var deserializedPaf = TestUtils.unpickle(serializedPaf, PropertiesAggregationFunction.class);
    var pvFromDeserializedPaf = deserializedPaf.apply(PropertyValue.create("1"), PropertyValue.create("2"));
    assertThat(pvFromDeserializedPaf, is(equalTo(pvFromPaf)));
    assertThat(deserializedPaf.getIdentity(), is(equalTo(paf.getIdentity())));
  }

}