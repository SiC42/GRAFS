package edu.leipzig.grafs.setup.reader;

import edu.leipzig.grafs.factory.VertexFactory;
import edu.leipzig.grafs.model.Vertex;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.Properties;

public class VertexReader extends CSVElementReader<Vertex> {

  public VertexReader(InputStream csvStream) throws FileNotFoundException {
    super(csvStream);
  }

  @Override
  Vertex parseGraphElem(String[] data) {
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

    var properties = getProperties(data[3]);

    return VertexFactory.initVertex(id, label, properties, graphIds);
  }

  private Properties getProperties(String propertiesStr) {
    String[] propertyStrArray = propertiesStr.split("\\|");
    var properties = Properties.create();
    if (propertyStrArray.length == 7) {
      properties.set("capacity", Integer.parseInt(propertyStrArray[0]));
      properties.set("id", propertyStrArray[1]);
      properties.set("lat", propertyStrArray[2]);
      properties.set("long", propertyStrArray[3]);
      properties.set("name", propertyStrArray[4]);
      properties.set("regionId", Short.parseShort(propertyStrArray[5]));
      properties.set("rentalURL", propertyStrArray[6]);
    }
    return properties;
  }

  public Map<GradoopId, Vertex> getVertices() throws IOException {
    Map<GradoopId, Vertex> graphElems = new HashMap<>();
    String row;
    while ((row = csvReader.readLine()) != null) {
      String[] data = row.split(DELIMITER);
      var graphElem = parseGraphElem(data);
      graphElems.put(graphElem.getId(), graphElem);
    }
    csvReader.close();
    return graphElems;
  }
}
