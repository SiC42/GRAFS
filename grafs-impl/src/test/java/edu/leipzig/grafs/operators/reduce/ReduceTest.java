package edu.leipzig.grafs.operators.reduce;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;

import org.hamcrest.collection.IsCollectionWithSize;

import edu.leipzig.grafs.model.Edge;
import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.model.Vertex;
import edu.leipzig.grafs.operators.OperatorTestBase;
import edu.leipzig.grafs.util.TestUtils;
import java.io.IOException;
import org.junit.jupiter.api.Test;

class ReduceTest extends OperatorTestBase {

  @Test
  public void testExecute() throws IOException {
    var loader = TestUtils.getSocialNetworkLoader();
    var stream = loader.createGCStream(getConfig());
    var reducedStream = stream.reduce();
    var result = reducedStream.collect();
    result.forEachRemaining(this::testIfTripletHasOneGraphId);
  }

  private void testIfTripletHasOneGraphId(Triplet<Vertex, Edge> triplet){
    assertThat(triplet.getEdge().getGraphIds(), hasSize(1));
    assertThat(triplet.getSourceVertex().getGraphIds(), hasSize(1));
    assertThat(triplet.getTargetVertex().getGraphIds(), hasSize(1));
  }

}