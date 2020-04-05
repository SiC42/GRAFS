package streaming.helper;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;
import streaming.model.Edge;
import streaming.model.GraphElementInformation;
import streaming.model.Vertex;

class AsciiGraphLoaderTest {

  private Collection<Edge> loadFromString_cycleGraph_init() {
    return AsciiGraphLoader.loadFromString(
        "[(v1:TestVertex)-[e1:TestEdge]->(v2:TestVertex) (v2:TestVertex)-[e2:TestEdge2]->(v1:TestVertex)]");
  }

  @Test
  void loadFromString_cycleGraph_testSize() throws Exception {
    Collection<Edge> edgeCollection = loadFromString_cycleGraph_init();
    assertThat(edgeCollection, hasSize(2));
  }

  @Test
  void loadFromString_cycleGraph_testVertices() throws Exception {
    Collection<Edge> edgeCollection = loadFromString_cycleGraph_init();
    Set<Vertex> fromList = new HashSet<>();
    Set<Vertex> toList = new HashSet<>();
    for (Edge e : edgeCollection) {
      fromList.add(e.getSource());
      toList.add(e.getTarget());
    }
    assertThat(fromList, equalTo(toList));
  }

  private Collection<Edge> loadFromString_oneEdgeWithInfo_init() {
    return AsciiGraphLoader.loadFromString(
        "[(alice:User {name: 'Alice'})-[e1:TestEdge {type: 'relationship'}]->(bob:User {name: 'Bob'})]");
  }

  @Test
  void loadFromString_oneEdgeWithInfo_testSize() {
    assertThat(loadFromString_oneEdgeWithInfo_init(), hasSize(1));
  }

  @Test
  void loadFromString_oneEdgeWithInfo_testVertices() {
    Edge e = loadFromString_oneEdgeWithInfo_init().iterator().next();
    GraphElementInformation from = e.getSource().getGei();
    GraphElementInformation to = e.getTarget().getGei();
    assertThat(from.getProperty("name"), equalTo("Alice"));
    assertThat(to.getProperty("name"), equalTo("Bob"));
  }
}