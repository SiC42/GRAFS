package edu.leipzig.grafs.operators;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import edu.leipzig.grafs.util.AsciiGraphLoader;
import edu.leipzig.grafs.util.FlinkConfig;
import edu.leipzig.grafs.util.TestUtils;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;

/**
 * Base class for Flink-based unit tests with the same cluster.
 */
public abstract class OperatorTestBase {

  private static final int DEFAULT_PARALLELISM = 4;

  private static final long TASKMANAGER_MEMORY_SIZE_MB = 512;

  @ClassRule
  public static MiniClusterWithClientResource miniClusterResource = getMiniCluster();
  /**
   * Flink Execution Environment
   */
  private final StreamExecutionEnvironment env;

  /**
   * Gradoop Flink configuration
   */
  private FlinkConfig config;

  /**
   * Creates a new instance of {@link OperatorTestBase}.
   */
  public OperatorTestBase() {
    this.env = StreamExecutionEnvironment.getExecutionEnvironment();
  }

  private static MiniClusterWithClientResource getMiniCluster() {
    Configuration config = new Configuration();
    config.setLong("taskmanager.memory.size", TASKMANAGER_MEMORY_SIZE_MB);

    return new MiniClusterWithClientResource(
        new MiniClusterResourceConfiguration.Builder()
            .setNumberTaskManagers(1)
            .setNumberSlotsPerTaskManager(DEFAULT_PARALLELISM)
            .setConfiguration(config).build());
  }

  /**
   * Returns the execution environment for the tests
   *
   * @return Flink execution environment
   */
  protected StreamExecutionEnvironment getExecutionEnvironment() {
    return env;
  }

  /**
   * Returns the configuration for the test
   *
   * @return Gradoop Flink configuration
   */
  protected FlinkConfig getConfig() {
    if (config == null) {
      setConfig(FlinkConfig.buildNewConfig(getExecutionEnvironment()).build());
    }
    return config;
  }

  /**
   * Sets the default configuration for the test
   */
  protected void setConfig(FlinkConfig config) {
    this.config = config;
  }

  //----------------------------------------------------------------------------
  // Data generation
  //----------------------------------------------------------------------------

  protected AsciiGraphLoader getLoaderFromString(String asciiString) {
    AsciiGraphLoader loader = AsciiGraphLoader.fromString(asciiString);
    loader.createEdgeStream(getConfig());
    return loader;
  }

  protected AsciiGraphLoader getLoaderFromFile(String fileName) throws IOException {
    AsciiGraphLoader loader = AsciiGraphLoader.fromFile(fileName);
    loader.createEdgeStream(getConfig());
    return loader;
  }

  protected AsciiGraphLoader getLoaderFromStream(InputStream inputStream) throws IOException {
    AsciiGraphLoader loader = AsciiGraphLoader.fromInputStream(inputStream);
    loader.createEdgeStream(getConfig());
    return loader;
  }

  /**
   * Creates a social network as a basis for tests.
   * <p/>
   * An image of the network can be found in gradoop/dev-support/social-network.pdf
   *
   * @return graph store containing a simple social network for tests.
   */
  protected AsciiGraphLoader getSocialNetworkLoader() throws IOException {
    InputStream inputStream = getClass()
        .getResourceAsStream(TestUtils.SOCIAL_NETWORK_GDL_FILE);
    return getLoaderFromStream(inputStream);
  }

  //----------------------------------------------------------------------------
  // Test helper
  //----------------------------------------------------------------------------

  protected void collectAndAssertTrue(DataSet<Boolean> result) throws Exception {
    assertTrue(result.collect().get(0), "expected true");
  }

  protected void collectAndAssertFalse(DataSet<Boolean> result) throws Exception {
    assertFalse(result.collect().get(0), "expected false");
  }

  /**
   * Returns the encoded file path to a resource.
   *
   * @param relPath the relative path to the resource
   * @return encoded file path
   */
  protected String getFilePath(String relPath) throws UnsupportedEncodingException {
    return URLDecoder.decode(
        getClass().getResource(relPath).getFile(), StandardCharsets.UTF_8.name());
  }
}
