package edu.leipzig.grafs.setup.reader;

import edu.leipzig.grafs.factory.EdgeFactory;
import edu.leipzig.grafs.model.Edge;
import edu.leipzig.grafs.model.Vertex;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Properties;

public class EdgeReader extends CSVElementReader<Edge> {

  public EdgeReader(InputStream csvStream) throws FileNotFoundException {
    super(csvStream);
  }

  @Override
  Edge parseGraphElem(String[] data) {
    var id = GradoopId.fromString(data[0]);

    var graphIds = getGraphIds(data[1]);

    var sourceId = GradoopId.fromString(data[2]);
    var targetId = GradoopId.fromString(data[3]);

    var label = data[4];

    var properties = getProperties(data[5]);

    return EdgeFactory.initEdge(id, label, sourceId, targetId, properties, graphIds);
  }

  private Properties getProperties(String propertiesStr) {
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

  public Edge getNextEdge() throws IOException {
    Map<GradoopId, Vertex> graphElems = new HashMap<>();
    String row;
    if ((row = csvReader.readLine()) != null) {
      String[] data = row.split(DELIMITER);
      var edge = parseGraphElem(data);
      return edge;
    } else {
      csvReader.close();
      return null;
    }
  }
}
