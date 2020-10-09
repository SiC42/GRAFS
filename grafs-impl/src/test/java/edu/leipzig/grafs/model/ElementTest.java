package edu.leipzig.grafs.model;

import static edu.leipzig.grafs.util.TestUtils.INT_VAL_2;
import static edu.leipzig.grafs.util.TestUtils.KEY_0;
import static edu.leipzig.grafs.util.TestUtils.KEY_1;
import static edu.leipzig.grafs.util.TestUtils.KEY_2;
import static edu.leipzig.grafs.util.TestUtils.LONG_VAL_3;
import static edu.leipzig.grafs.util.TestUtils.STRING_VAL_6;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EPGMElement;
import org.gradoop.common.model.impl.properties.Properties;
import org.junit.jupiter.api.Test;

public class ElementTest {

  @Test
  public void testSetId() {
    EPGMElement elementMock = mock(EPGMElement.class, CALLS_REAL_METHODS);
    GradoopId id = GradoopId.get();
    elementMock.setId(id);

    assertSame(id, elementMock.getId());
  }

  @Test
  public void testSetProperty() {
    Element elementMock = mock(Element.class, CALLS_REAL_METHODS);
    elementMock.setProperty(KEY_0, STRING_VAL_6);

    Properties properties = Properties.create();
    properties.set(KEY_0, STRING_VAL_6);

    assertEquals(elementMock.getProperties(), properties);
  }

  @Test
  public void testSetPropertyNull() {
    Element elementMock = mock(Element.class, CALLS_REAL_METHODS);
    assertThrows(NullPointerException.class, () -> elementMock.setProperty(null));
  }

  @Test
  public void testRemoveExistentProperty() {
    Properties properties = Properties.create();
    properties.set(KEY_0, STRING_VAL_6);
    GradoopId gradoopId = GradoopId.get();

    // create element mock with property
    Element elementMock = mock(Element.class, withSettings()
        .useConstructor(gradoopId, "someLabel", properties)
        .defaultAnswer(CALLS_REAL_METHODS));

    assertEquals(properties.get(KEY_0), elementMock.removeProperty(KEY_0));
    assertFalse(elementMock.hasProperty(KEY_0));
  }

  @Test
  public void testRemovePropertyNoProperties() {
    GradoopId gradoopId = GradoopId.get();

    // create element mock without properties
    Element elementMock = mock(Element.class, withSettings()
        .useConstructor(gradoopId, "someLabel", null)
        .defaultAnswer(CALLS_REAL_METHODS));

    assertNull(elementMock.removeProperty(KEY_1));
  }

  @Test
  public void testGetPropertyValueNull() {
    Properties properties = Properties.create();
    properties.set(KEY_0, STRING_VAL_6);
    GradoopId gradoopId = GradoopId.get();

    // create element mock with property
    Element elementMock = mock(Element.class, withSettings()
        .useConstructor(gradoopId, "someLabel", properties)
        .defaultAnswer(CALLS_REAL_METHODS));

    assertThrows(NullPointerException.class, () -> elementMock.getPropertyValue(null));
  }

  @Test
  public void testGetPropertyNoProperties() {
    GradoopId gradoopId = GradoopId.get();

    // create element mock without properties
    Element elementMock = mock(Element.class, withSettings()
        .useConstructor(gradoopId, "someLabel", null)
        .defaultAnswer(CALLS_REAL_METHODS));

    assertNull(elementMock.getPropertyValue(KEY_0));
  }

  @Test
  public void testHasPropertyNoProperties() {
    GradoopId gradoopId = GradoopId.get();

    // create element mock without properties
    Element elementMock = mock(Element.class, withSettings()
        .useConstructor(gradoopId, "someLabel", null)
        .defaultAnswer(CALLS_REAL_METHODS));

    assertFalse(elementMock.hasProperty(KEY_0));
  }

  @Test
  public void testGetPropertyKeysNoProperties() {
    GradoopId gradoopId = GradoopId.get();

    // create element mock without properties
    Element elementMock = mock(Element.class, withSettings()
        .useConstructor(gradoopId, "someLabel", null)
        .defaultAnswer(CALLS_REAL_METHODS));

    assertNull(elementMock.getPropertyKeys());
  }

  @Test
  public void testGetPropertyKeys() {
    Properties properties = Properties.create();
    properties.set(KEY_0, STRING_VAL_6);
    properties.set(KEY_1, INT_VAL_2);
    properties.set(KEY_2, LONG_VAL_3);
    GradoopId gradoopId = GradoopId.get();

    // create element mock with property
    Element elementMock = mock(Element.class, withSettings()
        .useConstructor(gradoopId, "someLabel", properties)
        .defaultAnswer(CALLS_REAL_METHODS));

    for (String key : elementMock.getPropertyKeys()) {
      assertTrue(key.equals(KEY_0) || key.equals(KEY_1) || key.equals(KEY_2));
    }
  }

}

