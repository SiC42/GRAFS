package edu.leipzig.grafs.operators.grouping.logic;


import static org.mockito.Mockito.mock;

import edu.leipzig.grafs.model.Vertex;
import edu.leipzig.grafs.operators.grouping.model.AggregatedVertex;
import edu.leipzig.grafs.util.TestUtils;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.junit.jupiter.api.Test;

class VertexAggregationProcessTest {

  @Test
  public void testAggregateVertex(){
    var vAggProcess = mock(VertexAggregationProcess.class);
    var aggV = new AggregatedVertex();
    var curV = new Vertex();
    aggV.setProperty(TestUtils.KEY_0, PropertyValue.create(TestUtils.INT_VAL_2));
    curV.setProperty(TestUtils.KEY_0, PropertyValue.create(TestUtils.INT_VAL_2));
  }
}