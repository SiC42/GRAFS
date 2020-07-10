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
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.junit.jupiter.api.Test;

class VertexAggregationProcessTest {

  @Test
  public void testAggregateVertex_curVertexNotAlreadyAggregated() {
    var vAggProcess = mock(VertexAggregationProcess.class, CALLS_REAL_METHODS);
    var aggKey = TestUtils.KEY_0;
    var aggFunc = TestUtils.INT_ADD_FUNC;
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
    var aggFunc = TestUtils.INT_ADD_FUNC;
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

  @Test
  public void testAggregateVertex_curVertexIsAggregatedVertex_curVertexHasAllInAggVertex() {
    var vAggProcess = mock(VertexAggregationProcess.class, CALLS_REAL_METHODS);
    var aggKey = TestUtils.KEY_0;
    var aggFunc = TestUtils.INT_ADD_FUNC;

    var id1 = GradoopId.get();
    var val1 = PropertyValue.create(TestUtils.INT_VAL_1);

    var id2 = GradoopId.get();
    var val2 = PropertyValue.create(TestUtils.INT_VAL_2);

    var id3 = GradoopId.get();
    var val3 = PropertyValue.create(TestUtils.INT_VAL_3);

    var aggMap = new AggregationMapping();
    aggMap.addAggregationForProperty(aggKey, aggFunc);

    var aggregatedVertex = new AggregatedVertex();
    aggregatedVertex.addVertex(id1);
    aggregatedVertex.addVertex(id2);
    aggregatedVertex.addVertex(id3);
    var partialAgg1n2 = aggFunc.apply(val1, val2);
    var fullyAggProp = aggFunc.apply(partialAgg1n2, val3);
    aggregatedVertex.setProperty(aggKey, partialAgg1n2);

    var currentVertex = new AggregatedVertex();
    aggregatedVertex.addVertex(id1);
    aggregatedVertex.addVertex(id2);
    aggregatedVertex.addVertex(id3);

    currentVertex.setProperty(aggKey, fullyAggProp);

    var expected = aggFunc.apply(partialAgg1n2, val3);

    var resultV = vAggProcess.aggregateVertex(aggMap, aggregatedVertex, currentVertex);
    assertThat(resultV.getPropertyValue(aggKey), is(equalTo(expected)));
  }

  @Test
  public void testAggregateVertex_curVertexIsAggregatedVertex_aggVertexHasAllInCurVertex() {
    var vAggProcess = mock(VertexAggregationProcess.class, CALLS_REAL_METHODS);
    var aggKey = TestUtils.KEY_0;
    var aggFunc = TestUtils.INT_ADD_FUNC;

    var id1 = GradoopId.get();
    var val1 = PropertyValue.create(TestUtils.INT_VAL_1);

    var id2 = GradoopId.get();
    var val2 = PropertyValue.create(TestUtils.INT_VAL_2);

    var id3 = GradoopId.get();
    var val3 = PropertyValue.create(TestUtils.INT_VAL_3);

    var aggMap = new AggregationMapping();
    aggMap.addAggregationForProperty(aggKey, aggFunc);

    var aggregatedVertex = new AggregatedVertex();
    aggregatedVertex.addVertex(id1);
    aggregatedVertex.addVertex(id2);
    aggregatedVertex.addVertex(id3);
    var partialAgg1n2 = aggFunc.apply(val1, val2);
    var fullyAggProp = aggFunc.apply(partialAgg1n2, val3);
    aggregatedVertex.setProperty(aggKey, fullyAggProp);

    var currentVertex = new AggregatedVertex();
    currentVertex.addVertex(id1);
    currentVertex.addVertex(id2);

    currentVertex.setProperty(aggKey, partialAgg1n2);

    var expected = aggFunc.apply(partialAgg1n2, val3);

    var resultV = vAggProcess.aggregateVertex(aggMap, aggregatedVertex, currentVertex);
    assertThat(resultV.getPropertyValue(aggKey), is(equalTo(expected)));
  }

  @Test
  public void testAggregateVertex_curVertexIsAggregatedVertex_hasPartialResult() {
    var vAggProcess = mock(VertexAggregationProcess.class, CALLS_REAL_METHODS);
    var aggKey = TestUtils.KEY_0;
    var aggFunc = TestUtils.INT_ADD_FUNC;

    var id1 = GradoopId.get();
    var val1 = PropertyValue.create(TestUtils.INT_VAL_1);

    var id2 = GradoopId.get();
    var val2 = PropertyValue.create(TestUtils.INT_VAL_2);

    var id3 = GradoopId.get();
    var val3 = PropertyValue.create(TestUtils.INT_VAL_3);

    var aggMap = new AggregationMapping();
    aggMap.addAggregationForProperty(aggKey, aggFunc);

    var aggregatedVertex = new AggregatedVertex();
    aggregatedVertex.addVertex(id1);
    aggregatedVertex.addVertex(id2);
    var partialAgg1n2 = aggFunc.apply(val1, val2);
    aggregatedVertex.setProperty(aggKey, partialAgg1n2);

    var currentVertex = new AggregatedVertex();
    currentVertex.addVertex(id1);
    currentVertex.addVertex(id3);
    var partialAgg1n3 = aggFunc.apply(val1, val3);
    currentVertex.setProperty(aggKey, partialAgg1n3);

    var expected = aggFunc.apply(partialAgg1n2, val3);

    var resultV = vAggProcess.aggregateVertex(aggMap, aggregatedVertex, currentVertex);
    assertThat(resultV.getPropertyValue(aggKey), is(equalTo(expected)));
  }

  @Test
  public void testAggregateVertex_curVertexIsAggregatedVertex_aggVertexHasNoVertexOfCurVertex() {
    var vAggProcess = mock(VertexAggregationProcess.class, CALLS_REAL_METHODS);
    var aggKey = TestUtils.KEY_0;
    var aggFunc = TestUtils.INT_ADD_FUNC;

    var id1 = GradoopId.get();
    var val1 = PropertyValue.create(TestUtils.INT_VAL_1);

    var id2 = GradoopId.get();
    var val2 = PropertyValue.create(TestUtils.INT_VAL_2);

    var id3 = GradoopId.get();
    var val3 = PropertyValue.create(TestUtils.INT_VAL_3);

    var aggMap = new AggregationMapping();
    aggMap.addAggregationForProperty(aggKey, aggFunc);

    var aggregatedVertex = new AggregatedVertex();
    aggregatedVertex.addVertex(id2);
    var partialAgg1n2 = aggFunc.apply(val1, val2);
    aggregatedVertex.setProperty(aggKey, partialAgg1n2);

    var currentVertex = new AggregatedVertex();
    currentVertex.addVertex(id1);
    var fullyAggProp = aggFunc.apply(partialAgg1n2, val3);
    currentVertex.setProperty(aggKey, fullyAggProp);

    var expected = aggFunc.apply(partialAgg1n2, val3);

    var resultV = vAggProcess.aggregateVertex(aggMap, aggregatedVertex, currentVertex);
    assertThat(resultV.getPropertyValue(aggKey), is(equalTo(expected)));
  }
}