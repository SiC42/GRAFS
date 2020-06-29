package streaming.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;
import streaming.model.Edge;
import streaming.model.EdgeContainer;
import streaming.model.Element;
import streaming.model.Vertex;

class AsciiGraphLoaderTest {

  private AsciiGraphLoader loader;

  public AsciiGraphLoaderTest(){
    loader = new AsciiGraphLoader();
  }

  private Collection<EdgeContainer> loadFromString_cycleGraph_init() {
    return loader.loadFromString(
        "[(v1:TestVertex)-[e1:TestEdge]->(v2:TestVertex) (v2:TestVertex)-[e2:TestEdge2]->(v1:TestVertex)]");
  }

  @Test
  void loadFromString_cycleGraph_testSize() throws Exception {
    Collection<EdgeContainer> edgeCollection = loadFromString_cycleGraph_init();
    assertThat(edgeCollection, hasSize(2));
  }

  @Test
  void loadFromString_cycleGraph_testVertices() throws Exception {
    Collection<EdgeContainer> edgeCollection = loadFromString_cycleGraph_init();
    Set<Vertex> fromList = new HashSet<>();
    Set<Vertex> toList = new HashSet<>();
    for (EdgeContainer e : edgeCollection) {
      fromList.add(e.getSourceVertex());
      toList.add(e.getTargetVertex());
    }
    assertThat(fromList, equalTo(toList));
  }

  private Collection<EdgeContainer> loadFromString_oneEdgeWithInfo_init() {
    return loader.loadFromString(
        "[(alice:User {name: 'Alice'})-[e1:TestEdge {type: 'relationship'}]->(bob:User {name: 'Bob'})]");
  }

  @Test
  void loadFromString_oneEdgeWithInfo_testSize() {
    assertThat(loadFromString_oneEdgeWithInfo_init(), hasSize(1));
  }

  @Test
  void loadFromString_oneEdgeWithInfo_testVertices() {
    EdgeContainer e = loadFromString_oneEdgeWithInfo_init().iterator().next();
    Element from = e.getSourceVertex();
    Element to = e.getTargetVertex();
    assertThat(from.getProperty("name"), equalTo("Alice"));
    assertThat(to.getProperty("name"), equalTo("Bob"));
  }
}