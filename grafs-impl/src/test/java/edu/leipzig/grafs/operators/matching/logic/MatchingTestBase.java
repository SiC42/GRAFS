package edu.leipzig.grafs.operators.matching.logic;

import edu.leipzig.grafs.model.Graph;
import edu.leipzig.grafs.model.Vertex;
import edu.leipzig.grafs.operators.matching.model.QueryGraph;
import edu.leipzig.grafs.util.AsciiGraphLoader;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;

public abstract class MatchingTestBase {

  // example after Figure 2 & Table 1 in
  // Miller, John A., et al.
  // "Research directions for big data graph analytics."
  // 2015 IEEE International Congress on Big Data. IEEE, 2015.
  protected final String paperGraphGdlStr = "all["
      // vertices
      // blue
      + "(v1:A {id: 1})"
      + "(v2:B {id: 2})"
      + "(v3:C {id: 3})"
      + "(v4:C {id: 4})"
      + "(v5:C {id: 5})"
      // green
      + "(v6:A {id: 6})"
      + "(v7:B {id: 7})"
      + "(v8:A {id: 8})"
      + "(v9:C {id: 9})"
      // white
      + "(v10:M {id: 10})"
      // red
      + "(v11:C {id: 11})"
      // pink
      + "(v12:A {id: 12})"
      + "(v13:B {id: 13})"
      + "(v14:C {id: 14})"
      // yellow
      + "(v15:B {id: 15})"
      + "(v16:A {id: 16})"
      + "(v17:B {id: 17})"
      + "(v18:C {id: 18})"
      + "(v19:A {id: 19})"
      + "(v20:A {id: 20})"
      + "(v21:B {id: 21})"
      + "(v22:C {id: 22})"
      // grey
      + "(v23:B {id: 23})"
      + "(v24:A {id: 24})"
      + "(v25:C {id: 25})"
      + "(v26:B {id: 26})"
      + "(v27:A {id: 27})"
      + "(v28:C {id: 28})"
      + "(v29:B {id: 29})"
      + "(v30:A {id: 30})"
      // edges
      // from blue
      + "(v1)-[e1]->(v2)"
      + "(v2)-[e2]->(v1)"
      + "(v2)-[e3]->(v3)"
      + "(v2)-[e4]->(v4)"
      + "(v2)-[e5]->(v5)"
      + "(v2)-[e6]->(v6)"
      // from green
      + "(v6)-[e7]->(v7)"
      + "(v6)-[e8]->(v11)"
      + "(v7)-[e9]->(v8)"
      + "(v7)-[e10]->(v5)"
      + "(v7)-[e11]->(v9)"
      + "(v7)-[e12]->(v10)"
      + "(v8)-[e13]->(v2)"
      // from red
      + "(v11)-[e14]->(v12)"
      // from pink
      + "(v12)-[e15]->(v13)"
      + "(v13)-[e16]->(v12)"
      + "(v13)-[e17]->(v14)"
      // from yellow
      + "(v15)-[e18]->(v14)"
      + "(v15)-[e19]->(v16)"
      + "(v16)-[e20]->(v17)"
      + "(v17)-[e21]->(v18)"
      + "(v17)-[e22]->(v19)"
      + "(v18)-[e23]->(v15)"
      + "(v18)-[e24]->(v20)"
      + "(v19)-[e25]->(v21)"
      + "(v20)-[e26]->(v15)"
      + "(v21)-[e27]->(v20)"
      + "(v21)-[e28]->(v22)"
      // from grey
      + "(v23)-[e29]->(v22)"
      + "(v23)-[e30]->(v24)"
      + "(v24)-[e31]->(v26)"
      + "(v26)-[e32]->(v25)"
      + "(v26)-[e33]->(v27)"
      + "(v27)-[e34]->(v29)"
      + "(v29)-[e35]->(v28)"
      + "(v29)-[e36]->(v30)"
      + "(v30)-[e37]->(v23)]"
      + "g1[(v1) (v2) (v3) (v4)"
      + "(v1)-[e1]->(v2) (v2)-[e2]->(v1) (v2)-[e3]->(v3) (v2)-[e4]->(v3)]"
      + "g2[(v1) (v2) (v3) (v5)"
      + "(v1)-[e1]->(v2) (v2)-[e2]->(v1) (v2)-[e3]->(v3) (v2)-[e5]->(v5)]"
      + "g3[(v1) (v2) (v4) (v5)"
      + "(v1)-[e1]->(v2) (v2)-[e2]->(v1) (v2)-[e4]->(v4) (v2)-[e5]->(v5)]";
  protected AsciiGraphLoader graphLoader;
  protected Graph graph;
  protected AsciiGraphLoader queryLoader;
  protected Graph queryGraph;
  protected String queryPaperGraphGdlStr = ""
      // vertices
      + "(qa1:A)"
      + "(qb2:B)"
      + "(qc3:C)"
      + "(qc4:C)"
      //edges
      + "(qa1)-[]->(qb2)"
      + "(qb2)-[]->(qa1)"
      + "(qb2)-[]->(qc3)"
      + "(qb2)-[]->(qc4)";

  @BeforeEach
  public void initLoadersAndGraphs() {
    graphLoader = AsciiGraphLoader.fromString(paperGraphGdlStr);
    graph = graphLoader.createGraph();
    queryLoader = AsciiGraphLoader.fromString(queryPaperGraphGdlStr);
    queryGraph = queryLoader.createGraph();
  }

  Set<Vertex> filterBySelfAssignedId(Collection<Vertex> vertices, int... ids) {
    Set<Vertex> filteredVertex = new HashSet<>();
    for (var vertex : vertices) {
      for (var id : ids) {
        if (vertex.getPropertyValue("id").toString().equals(Integer.toString(id))) {
          filteredVertex.add(vertex);
          break;
        }
      }
    }
    return filteredVertex;
  }


}
