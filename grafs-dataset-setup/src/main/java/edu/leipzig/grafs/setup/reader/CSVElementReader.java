package edu.leipzig.grafs.setup.reader;

import edu.leipzig.grafs.model.GraphElement;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashSet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;

public abstract class CSVElementReader<E extends GraphElement> {

  protected static final String DELIMITER = ";";

  protected final BufferedReader csvReader;

  public CSVElementReader(InputStream csvStream) throws FileNotFoundException {
    this.csvReader = new BufferedReader(new InputStreamReader(csvStream));
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

  abstract E parseGraphElem(String[] data);

}
