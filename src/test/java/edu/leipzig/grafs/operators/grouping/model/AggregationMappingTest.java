package edu.leipzig.grafs.operators.grouping.model;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import edu.leipzig.grafs.operators.grouping.functions.SerializableBiFunction;
import edu.leipzig.grafs.util.TestUtils;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.Random;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.junit.jupiter.api.Test;

public class AggregationMappingTest {


  @Test
  public void testAddAggregationForProperty() {
    var am = new AggregationMapping();
    var paf = TestUtils.INT_ADD_FUNC.apply("a");
    var key = TestUtils.KEY_1;
    am.addAggregationForProperty(key, paf);
    assertThat(am.getAggregationForProperty(key), is(equalTo(paf)));
  }

  @Test
  public void testContainsAggregationForProperty() {
    var am = new AggregationMapping();
    var paf = TestUtils.INT_ADD_FUNC.apply("a");
    var key = TestUtils.KEY_1;
    am.addAggregationForProperty(key, paf);
    assertThat(am.containsAggregationForProperty(key), is(true));
  }

  @Test
  public void testGetAggregationForMembership_empty() {
    var am = new AggregationMapping();
    assertThat(am.getAggregationForMembership(), is(equalTo(Optional.empty())));
  }

  @Test
  public void testGetAggregationForMembership_notEmpty() {
    var am = new AggregationMapping();
    SerializableBiFunction<GradoopIdSet, GradoopIdSet, GradoopIdSet> membershipAggregation =
        (id1, id2) -> id1;
    am.setAggregationForMembership(membershipAggregation);
    assertThat(am.getAggregationForMembership().isPresent(), is(true));
    assertThat(am.getAggregationForMembership().get(), is(equalTo(membershipAggregation)));

  }

  @Test
  public void testEntrySet() {
    var am = new AggregationMapping();
    var key1 = TestUtils.KEY_1;
    var paf1 = TestUtils.INT_ADD_FUNC.apply("a");
    var key2 = TestUtils.KEY_2;
    var paf2 = TestUtils.STRING_CONC_FUNC.apply("a");
    am.addAggregationForProperty(key1, paf1);
    am.addAggregationForProperty(key2, paf2);
    assertThat(am.entrySet(), hasSize(2));
    for (var entry : am.entrySet()) {
      if (entry.getPropertyKey().equalsIgnoreCase(key1)) {
        assertThat(entry.getAggregationFunction(), is(equalTo(paf1)));
      } else {
        assertThat(entry.getAggregationFunction(), is(equalTo(paf2)));
      }
    }
  }

  @Test
  public void testSerialization() throws IOException, ClassNotFoundException {
    var am = new AggregationMapping();
    var key1 = TestUtils.KEY_1;
    var propAggFunc1 = TestUtils.INT_ADD_FUNC.apply("a");
    var key2 = TestUtils.KEY_2;
    var propAggFunc2 = TestUtils.STRING_CONC_FUNC.apply("a");
    am.addAggregationForProperty(key1, propAggFunc1);
    am.addAggregationForProperty(key2, propAggFunc2);

    byte[] serializedAm = TestUtils.pickle(am);
    var deserializedAm = TestUtils.unpickle(serializedAm, AggregationMapping.class);
    assertThat(deserializedAm.entrySet(), hasSize(2));

    var deserializedPropAggFunc1 = deserializedAm.getAggregationForProperty(key1);
    var deserializedPropAggFunc2 = deserializedAm.getAggregationForProperty(key2);

    assertThat(deserializedPropAggFunc1.getIdentity(), is(equalTo(propAggFunc1.getIdentity())));
    assertThat(deserializedPropAggFunc2.getIdentity(), is(equalTo(propAggFunc2.getIdentity())));

    var random = new Random();
    for (int i = 0; i < 1000; i++) {
      var randomInt1 = random.nextInt(Integer.MAX_VALUE / 2);
      var randomInt2 = random.nextInt(Integer.MAX_VALUE / 2);
      var expectedResult = propAggFunc1
          .apply(PropertyValue.create(randomInt1), PropertyValue.create(randomInt2));
      var actualResult = deserializedPropAggFunc1
          .apply(PropertyValue.create(randomInt1), PropertyValue.create(randomInt2));
      assertThat(actualResult, is(equalTo(expectedResult)));
    }
    for (int i = 0; i < 1000; i++) {
      var randomString1 = generateRandomString();
      var randomString2 = generateRandomString();
      var expectedResult = propAggFunc2
          .apply(PropertyValue.create(randomString1), PropertyValue.create(randomString2));
      var actualResult = deserializedPropAggFunc2
          .apply(PropertyValue.create(randomString1), PropertyValue.create(randomString2));
      assertThat(actualResult, is(equalTo(expectedResult)));
    }

  }

  private String generateRandomString() {
    byte[] array = new byte[7]; // length is bounded by 7
    new Random().nextBytes(array);
    return new String(array, StandardCharsets.UTF_8);
  }
}