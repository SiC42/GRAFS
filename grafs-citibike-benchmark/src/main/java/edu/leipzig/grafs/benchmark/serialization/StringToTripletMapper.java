package edu.leipzig.grafs.benchmark.serialization;

import edu.leipzig.grafs.factory.EdgeFactory;
import edu.leipzig.grafs.factory.VertexFactory;
import edu.leipzig.grafs.model.Edge;
import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.model.Vertex;
import java.util.Arrays;
import java.util.HashSet;
import java.util.function.Function;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.io.ParseException;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.Properties;


public class StringToTripletMapper implements MapFunction<String,Triplet<Vertex,Edge>>,
    Function<String,Triplet<Vertex,Edge>> {

  public static final String DELIMITER = ";";


  @Override
  public Triplet<Vertex,Edge> apply(String tripletString) {
    return map(tripletString);
  }

  @Override
  public Triplet<Vertex,Edge> map(String tripletString) {
    String[] tripletArray = tripletString.split("\t");
    if(tripletArray.length != 3){
      throw new ParseException("Malformed triplet. Could not find all elements of a triplet");
    }
    Vertex sourceVertex;
    Vertex targetVertex;
    Edge edge;
    try {
      sourceVertex = parserVertex(tripletArray[0]);
    } catch (Exception e){
      throw new ParseException(String.format("Malformed source vertex string: '%s'", tripletArray[0]),e);
    }
    try{
    targetVertex = parserVertex(tripletArray[2]);
    } catch (Exception e){
      throw new ParseException(String.format("Malformed target vertex string: '%s'", tripletArray[2]),e);
    }
    try{
      edge = parseEdge(tripletArray[1]);
    } catch (Exception e){
      e.printStackTrace();
      throw new ParseException(String.format("Malformed edge string: '%s'", tripletArray[1]),e);
    }
    return new Triplet<>(edge, sourceVertex, targetVertex);
  }

  private Vertex parserVertex(String vertexStr) {
    String[] data = splitCsv(vertexStr);
    GradoopId id;
    GradoopIdSet graphIds;
    try {
      id = GradoopId.fromString(data[0]);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          e.getMessage() + " in id of element " + Arrays.toString(data));
    }
    try {
      graphIds = getGraphIds(data[1]);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          e.getMessage() + " in graphIds of element " + Arrays.toString(data));
    }
    var label = data[2];

    var properties = getVertexProperties(data[3]);

    return VertexFactory.initVertex(id, label, properties, graphIds);
  }

  private String[] splitCsv(String elemStr) {
    return elemStr.split(DELIMITER);
  }

  private Properties getVertexProperties(String propertiesStr) {
    String[] propertyStrArray = propertiesStr.split("\\|");
    var properties = Properties.create();
    try {
      if (!propertyStrArray[0].equals("")) {
        properties.set("capacity", Integer.parseInt(propertyStrArray[0]));
      }
      properties.set("id", propertyStrArray[1]);

      var latStr = propertyStrArray[2];
      float lat = !latStr.equals("") && ! latStr.equals("NULL")  ? Float.parseFloat(latStr) : -1;
      properties.set("lat", lat);

      var lonStr = propertyStrArray[3];
      float lon = !lonStr.equals("") && ! lonStr.equals("NULL") ? Float.parseFloat(lonStr) : -1;
      properties.set("long", lon);

      properties.set("name", propertyStrArray[4]);
      if (propertyStrArray.length > 5 && !propertyStrArray[5].equals("")) {
        properties.set("regionId", Integer.parseInt(propertyStrArray[5]));
      }
      if (propertyStrArray.length > 6) {
        properties.set("rentalURL", propertyStrArray[6]);
      }
    } catch (IndexOutOfBoundsException e){
      System.out.println(propertiesStr);
      throw e;
    }
    return properties;
  }

  private Edge parseEdge(String edgeStr) {
    String[] data = splitCsv(edgeStr);
    var id = GradoopId.fromString(data[0]);

    var graphIds = getGraphIds(data[1]);

    var sourceId = GradoopId.fromString(data[2]);
    var targetId = GradoopId.fromString(data[3]);

    var label = data[4];

    var properties = getEdgeProperties(data[5]);

    return EdgeFactory.initEdge(id, label, sourceId, targetId, properties, graphIds);
  }

  private Properties getEdgeProperties(String propertiesStr) {
    String[] propertyStrArray = propertiesStr.split("\\|");
    var properties = Properties.create();
    if (propertyStrArray.length == 7) {
      properties.set("bike_id", propertyStrArray[0]);
      properties.set("gender", propertyStrArray[1]);
      properties.set("starttime", propertyStrArray[2]);
      properties.set("stoptime", propertyStrArray[3]);
      properties.set("tripduration", propertyStrArray[4]);
      properties.set("user_type", propertyStrArray[5]);
      properties.set("year_birth", propertyStrArray[6]);
    }
    return properties;
  }


  GradoopIdSet getGraphIds(String graphIdsStr) {
    String[] ids = graphIdsStr.split(",");
    var graphIds = new HashSet<GradoopId>();
    int end = ids.length - 1;
    if (end == 0) { // there is only one graph id, so remove the brackets
      var id = ids[0];
      id = id.substring(1, id.length() - 1);
      graphIds.add(GradoopId.fromString(id));
      return GradoopIdSet.fromExisting(graphIds);
    } else {
      graphIds.add(GradoopId.fromString(ids[0].substring(1)));
    }
    for (int i = 1; i < end; i++) {
      graphIds.add(GradoopId.fromString(ids[i]));
    }
    var lastId = ids[end];
    lastId = lastId.substring(0, lastId.length() - 1);
    graphIds.add(GradoopId.fromString(lastId));
    return GradoopIdSet.fromExisting(graphIds);
  }
}
