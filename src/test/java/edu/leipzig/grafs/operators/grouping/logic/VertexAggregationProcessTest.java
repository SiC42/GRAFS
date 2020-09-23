package edu.leipzig.grafs.operators.grouping.logic;


import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;

import edu.leipzig.grafs.model.Vertex;
import edu.leipzig.grafs.operators.grouping.model.AggregatedVertex;
import edu.leipzig.grafs.operators.grouping.model.AggregationMapping;
import edu.leipzig.grafs.util.TestUtils;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.junit.jupiter.api.Test;

class VertexAggregationProcessTest {

  @Test
  public void testAggregateVertex_curVertexNotAlreadyAggregated() {
    var vAggProcess = mock(VertexAggregationProcess.class, CALLS_REAL_METHODS);
    var aggKey = TestUtils.KEY_0;
    var aggFunc = TestUtils.INT_ADD_FUNC.apply(TestUtils.KEY_0);
    var val1 = PropertyValue.create(TestUtils.INT_VAL_1);
    var val2 = PropertyValue.create(TestUtils.INT_VAL_2);

    var aggMap = new AggregationMapping();
    aggMap.addAggregationForProperty(aggKey, aggFunc);
    var aggV = new AggregatedVertex();
    var curV = new Vertex();
    aggV.setProperty(aggKey, val1);
    curV.setProperty(aggKey, val2);
    var expectedProp = aggFunc.apply(val1, val2);

    var resultV = vAggProcess.aggregateVertex(aggMap, aggV, curV);

    assertThat(resultV.getProperties().size(), is(1));
    assertThat(resultV.getPropertyValue(aggKey), is(equalTo(expectedProp)));

  }

  @Test
  public void testAggregateVertex_curVertexAlreadyAggregated() {
    var vAggProcess = mock(VertexAggregationProcess.class, CALLS_REAL_METHODS);
    var aggKey = TestUtils.KEY_0;
    var aggFunc = TestUtils.INT_ADD_FUNC.apply(TestUtils.KEY_0);
    var val1 = PropertyValue.create(TestUtils.INT_VAL_1);
    var val2 = PropertyValue.create(TestUtils.INT_VAL_2);

    var aggMap = new AggregationMapping();
    aggMap.addAggregationForProperty(aggKey, aggFunc);
    var aggV = new AggregatedVertex();
    var curV = new Vertex();
    aggV.addVertex(curV);
    aggV.setProperty(aggKey, val1);
    curV.setProperty(aggKey, val2);
    var expected = aggV.getPropertyValue(aggKey);

    var resultV = vAggProcess.aggregateVertex(aggMap, aggV, curV);
    assertThat(resultV.getPropertyValue(aggKey), is(equalTo(expected)));

  }
}