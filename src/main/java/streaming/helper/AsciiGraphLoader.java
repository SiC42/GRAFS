package streaming.helper;

import org.s1ck.gdl.GDLHandler;
import org.s1ck.gdl.model.Vertex;
import streaming.model.Edge;

import java.util.*;

public class AsciiGraphLoader {

    public static Collection<Edge> loadFromString(String graphStr) {
        GDLHandler handler = new GDLHandler.Builder().buildFromString(graphStr);
        Set<Edge> edges = new HashSet<>();
        Map<Long, Vertex> verticesDic = new HashMap<>();
        for(Vertex v : handler.getVertices()) {
            verticesDic.put(v.getId(),v);
        }
        for(org.s1ck.gdl.model.Edge gdlE : handler.getEdges()){
            long source = gdlE.getSourceVertexId();
            Vertex sourceV = verticesDic.get(source);
            long target = gdlE.getTargetVertexId();
            Vertex targetV = verticesDic.get(target);
            Edge e = new streaming.model.Edge(gdlE,sourceV,targetV);
            edges.add(e);
        }
        return edges;
    }
}
