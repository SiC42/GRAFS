package edu.leipzig.grafs.operators.grouping.logic;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;

import edu.leipzig.grafs.model.Vertex;
import edu.leipzig.grafs.operators.grouping.functions.AggregateFunction;
import edu.leipzig.grafs.operators.grouping.model.GroupingInformation;
import edu.leipzig.grafs.util.TestUtils;
import java.util.HashSet;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.junit.jupiter.api.Test;

class ElementAggregationTest {

  @Test
  void testCheckAggregationAndGroupingKeyIntersection_withKeyConflict() {
    var aggMap = new HashSet<AggregateFunction>();
    aggMap.add(TestUtils.STRING_CONC_FUNC.apply(TestUtils.KEY_0));
    var groupInfo = new GroupingInformation();
    groupInfo.addKey(TestUtils.KEY_1);

    var aggProcess = mock(ElementAggregation.class, CALLS_REAL_METHODS);
    aggProcess.checkAggregationAndGroupingKeyIntersection(aggMap, groupInfo);

    groupInfo.addKey(TestUtils.KEY_0);

    assertThrows(RuntimeException.class, () ->
        aggProcess.checkAggregationAndGroupingKeyIntersection(aggMap, groupInfo));
  }

  @Test
  void testSetGroupedProperties_noGroupInfo() {
    var aggProcess = mock(ElementAggregation.class, CALLS_REAL_METHODS);
    var emptyVertex = new Vertex();
    emptyVertex.setProperty(TestUtils.KEY_0, PropertyValue.create(TestUtils.INT_VAL_2));
    var masterVertex = new Vertex();
    masterVertex.setProperty(TestUtils.KEY_1, PropertyValue.create(5));

    var resultVertex = aggProcess.setGroupedProperties(null, emptyVertex, masterVertex);

    assertThat(resultVertex, is(equalTo(emptyVertex)));
  }

  @Test
  void testSetGroupedProperties_ShouldUseLabel() {
    var aggProcess = mock(ElementAggregation.class, CALLS_REAL_METHODS);
    var groupInfo = new GroupingInformation();
    groupInfo.useLabel(true);

    var emptyVertex = new Vertex();
    emptyVertex.setProperty(TestUtils.KEY_0, PropertyValue.create(TestUtils.INT_VAL_2));
    var masterVertex = new Vertex();
    masterVertex.setLabel("hello");
    masterVertex.setProperty(TestUtils.KEY_1, PropertyValue.create(5));

    var resultVertex = aggProcess.setGroupedProperties(groupInfo, emptyVertex, masterVertex);

    assertThat(resultVertex.getLabel(), is(equalTo(masterVertex.getLabel())));
    assertThat(resultVertex.getProperties(), is(equalTo(emptyVertex.getProperties())));
  }

  @Test
  void testSetGroupedProperties_onPropertyKeys() {
    var aggProcess = mock(ElementAggregation.class, CALLS_REAL_METHODS);
    var groupInfo = new GroupingInformation();
    var groupKey = TestUtils.KEY_0;
    groupInfo.addKey(groupKey);

    var emptyVertex = new Vertex();
    emptyVertex.setProperty(TestUtils.KEY_1, PropertyValue.create(TestUtils.INT_VAL_2));
    var masterVertex = new Vertex();
    masterVertex.setLabel("hello");
    masterVertex.setProperty(groupKey, PropertyValue.create(5));

    var resultVertex = aggProcess.setGroupedProperties(groupInfo, emptyVertex, masterVertex);

    assertThat(resultVertex.getLabel(), is(equalTo(emptyVertex.getLabel())));
    for (var key : resultVertex.getPropertyKeys()) {
      if (masterVertex.hasProperty(key)) {
        assertThat(resultVertex.getPropertyValue(key),
            is(equalTo(masterVertex.getPropertyValue(key))));
      } else {
        assertThat(resultVertex.getPropertyValue(key),
            is(equalTo(emptyVertex.getPropertyValue(key))));
      }
    }
  }


  @Test
  void testAggregateGraphElement_aggElementHasProperty() {
    var aggProcess = mock(ElementAggregation.class, CALLS_REAL_METHODS);
    var aggKey = TestUtils.KEY_0;

    var aggMap = new HashSet<AggregateFunction>();
    var func = TestUtils.INT_ADD_FUNC.apply(aggKey);
    aggMap.add(func);

    var prop1 = PropertyValue.create(TestUtils.INT_VAL_1);
    var prop2 = PropertyValue.create(TestUtils.INT_VAL_2);

    var curVertex = new Vertex();
    curVertex.setProperty(aggKey, prop1);
    curVertex.setProperty(TestUtils.KEY_1, prop1);
    var aggVertex = new Vertex();
    aggVertex.setProperty(aggKey, prop2);

    var expected = func.aggregate(prop1.copy(), prop2.copy());

    var resultVertex = aggProcess.aggregateElement(aggVertex, curVertex, aggMap);

    assertThat(resultVertex.getProperties().size(), is(1));
    assertThat(resultVertex.getPropertyValue(aggKey), is(equalTo(expected)));
  }

  @Test
  void testAggregateGraphElement_aggElementDoesntHaveProperty() {
    var aggProcess = mock(ElementAggregation.class, CALLS_REAL_METHODS);
    var aggKey = TestUtils.KEY_0;

    var aggMap = new HashSet<AggregateFunction>();
    var func = TestUtils.INT_ADD_FUNC.apply(TestUtils.KEY_0);
    aggMap.add(func);

    var prop1 = PropertyValue.create(TestUtils.INT_VAL_1);
    var prop2 = PropertyValue.create(TestUtils.INT_VAL_2);

    var curVertex = new Vertex();
    curVertex.setProperty(aggKey, prop1);
    curVertex.setProperty(TestUtils.KEY_1, prop1);
    var aggVertex = new Vertex();

    var expected = func.aggregate(PropertyValue.create(0), prop1);

    var resultVertex = aggProcess.aggregateElement(aggVertex, curVertex, aggMap);

    assertThat(resultVertex.getProperties().size(), is(1));
    assertThat(resultVertex.getPropertyValue(aggKey), is(equalTo(expected)));
  }

  @Test
  void testAggregateGraphElement_wontDeleteProps() {
    var aggProcess = mock(ElementAggregation.class, CALLS_REAL_METHODS);
    var aggKey = TestUtils.KEY_0;

    var aggMap = new HashSet<AggregateFunction>();
    var func = TestUtils.INT_ADD_FUNC.apply(TestUtils.KEY_0);
    aggMap.add(func);

    var prop1 = PropertyValue.create(TestUtils.INT_VAL_1);
    var prop2 = PropertyValue.create(TestUtils.INT_VAL_2);

    var curVertex = new Vertex();
    curVertex.setProperty(aggKey, prop1);
    curVertex.setProperty(TestUtils.KEY_1, prop1);
    var aggVertex = new Vertex();
    aggVertex.setProperty(aggKey, prop2);

    var notTouchedPropKey = TestUtils.KEY_2;
    aggVertex.setProperty(notTouchedPropKey, prop2);

    var resultVertex = aggProcess.aggregateElement(aggVertex, curVertex, aggMap);

    assertThat(resultVertex.getProperties().size(), is(2));
    assertThat(resultVertex.getPropertyValue(notTouchedPropKey),
        is(equalTo(aggVertex.getPropertyValue(notTouchedPropKey))));
  }
}