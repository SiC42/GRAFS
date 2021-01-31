package edu.leipzig.grafs.benchmark.config;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

public class ProducerConfig {

  private static final String DEFAULT_PROP_FILE = "default.properties";
  private static final String LOCAL_DEFAULT_PROP_FILE = "benchmark.properties";


  public static Properties loadDefaultProperties() {
    var props = new Properties();

    props.putAll(loadDefaultPropertiesInternal());

    // load from local file
    var path = Paths.get(LOCAL_DEFAULT_PROP_FILE);
    if (Files.exists(path)) {
      System.out.println("Found local properties file. Loading.");
      try (var inputStream = Files.newInputStream(path)) {
        props.load(inputStream);
      } catch (IOException ignored) {
      }
    }
    return props;
  }

  private static Properties loadDefaultPropertiesInternal() {
    var props = new Properties();

    try (var inputStream = ProducerConfig.class.getClassLoader()
        .getResourceAsStream(DEFAULT_PROP_FILE)) {
      props.load(inputStream);
    } catch (IOException ignored) {
    }

    return props;
  }

  public static Properties loadProperties(String propFileLocation) throws IOException {
    var props = loadDefaultProperties();
    var path = Paths.get(propFileLocation);
    try (var inputStream = Files.newInputStream(path)) {
      props.load(inputStream);
    } catch (IOException e) {
      throw e;
    }
    return props;
  }

}
