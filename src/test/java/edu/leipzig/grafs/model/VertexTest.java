/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.leipzig.grafs.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import edu.leipzig.grafs.factory.VertexFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.util.GradoopConstants;
import org.junit.jupiter.api.Test;

public class VertexTest {

  @Test
  public void createWithIDTest() {
    GradoopId vertexID = GradoopId.get();
    Vertex v = new VertexFactory().initVertex(vertexID);
    assertEquals(v.getId(), vertexID);
    assertEquals(v.getPropertyCount(), 0);
    assertEquals(v.getGraphCount(), 0);
  }

  @Test
  public void createVertexPojoTest() {
    GradoopId vertexID = GradoopId.get();
    String label = "A";
    Properties props = Properties.create();
    props.set("k1", "v1");
    props.set("k2", "v2");

    GradoopId graphId1 = GradoopId.get();
    GradoopId graphId2 = GradoopId.get();

    GradoopIdSet graphIds = new GradoopIdSet();
    graphIds.add(graphId1);
    graphIds.add(graphId2);

    Vertex vertex = new VertexFactory()
        .initVertex(vertexID, label, props, graphIds);

    assertEquals(vertex.getId(), vertexID);
    assertEquals(label, vertex.getLabel());
    assertEquals(vertex.getPropertyCount(), 2);
    assertEquals(vertex.getPropertyValue("k1").getString(), "v1");
    assertEquals(vertex.getPropertyValue("k2").getString(), "v2");
    assertEquals(vertex.getGraphCount(), 2);
    assertTrue(vertex.getGraphIds().contains(graphId1));
    assertTrue(vertex.getGraphIds().contains(graphId2));
  }

  @Test
  public void createWithMissingLabelTest() {
    GradoopId vertexID = GradoopId.get();
    Vertex v = new VertexFactory().initVertex(vertexID);
    assertEquals(v.getLabel(), GradoopConstants.DEFAULT_VERTEX_LABEL);
  }

  @Test
  public void createWithNullIDTest() {
    assertThrows(NullPointerException.class, () -> new VertexFactory().initVertex(null));
  }

  @Test
  public void createWithNullLabelTest() {
    GradoopId vertexID = GradoopId.get();
    assertThrows(NullPointerException.class, () -> new VertexFactory().initVertex(vertexID, null));
  }
}
