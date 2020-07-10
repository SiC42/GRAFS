package edu.leipzig.grafs.model;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.Properties;
import org.junit.jupiter.api.Test;

public class GraphElementTest {

  @Test
  public void testAddGraphIdNoGraphIds() {
    GraphElement graphElementMock = mock(GraphElement.class, CALLS_REAL_METHODS);
    graphElementMock.setGraphIds(new GradoopIdSet());

    GradoopId id = GradoopId.get();
    graphElementMock.addGraphId(id);

    assertNotNull(graphElementMock.getGraphIds());
  }

  @Test
  public void testResetGraphIds() {
    Properties propertiesMock = mock(Properties.class);
    GradoopIdSet idSet = new GradoopIdSet();
    idSet.add(GradoopId.get());

    GraphElement graphElementMock = mock(GraphElement.class, withSettings()
        .useConstructor(GradoopId.get(), "someLabel", propertiesMock, idSet)
        .defaultAnswer(CALLS_REAL_METHODS));

    graphElementMock.resetGraphIds();

    assertTrue(graphElementMock.getGraphIds().isEmpty());
  }
}

