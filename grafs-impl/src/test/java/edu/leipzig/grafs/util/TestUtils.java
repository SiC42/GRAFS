package edu.leipzig.grafs.util;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.google.common.collect.Maps;
import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.model.streaming.BaseStream;
import edu.leipzig.grafs.model.streaming.GraphStream;
import edu.leipzig.grafs.model.Element;
import edu.leipzig.grafs.model.GraphElement;
import edu.leipzig.grafs.operators.grouping.functions.AggregateFunction;
import edu.leipzig.grafs.operators.grouping.functions.BaseAggregateFunction;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.function.Function;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.common.model.impl.properties.PropertyValueUtils;


public class TestUtils {

  public static final String SOCIAL_NETWORK_GDL_FILE = "/data/gdl/social_network.gdl";
  public static final String KEY_0 = "key0";
  public static final String KEY_1 = "key1";
  public static final String KEY_2 = "key2";
  public static final String KEY_3 = "key3";
  public static final String KEY_4 = "key4";
  public static final String KEY_5 = "key5";
  public static final String KEY_6 = "key6";
  public static final String KEY_7 = "key7";
  public static final String KEY_8 = "key8";
  public static final String KEY_9 = "key9";
  public static final String KEY_a = "keya";
  public static final String KEY_b = "keyb";
  public static final String KEY_c = "keyc";
  public static final String KEY_d = "keyd";
  public static final String KEY_e = "keye";
  public static final String KEY_f = "keyf";
  public static final String KEY_g = "keyg";
  public static final String KEY_h = "keyh";
  public static final Object NULL_VAL_0 = null;
  public static final boolean BOOL_VAL_1 = true;
  public static final int INT_VAL_1 = 50;
  public static final int INT_VAL_2 = 23;
  public static final int INT_VAL_3 = 80;
  public static final long LONG_VAL_3 = 23L;
  public static final float FLOAT_VAL_4 = 2.3f;
  public static final double DOUBLE_VAL_5 = 2.3;
  public static final String STRING_VAL_6 = "23";
  public static final BigDecimal BIG_DECIMAL_VAL_7 = new BigDecimal(23);
  public static final GradoopId GRADOOP_ID_VAL_8 = GradoopId.get();
  public static final Map<PropertyValue, PropertyValue> MAP_VAL_9 = new HashMap<>();
  public static final List<PropertyValue> LIST_VAL_a = new ArrayList<>();
  public static final LocalDate DATE_VAL_b = LocalDate.now();
  public static final LocalTime TIME_VAL_c = LocalTime.now();
  public static final LocalDateTime DATETIME_VAL_d = LocalDateTime.now();
  public static final short SHORT_VAL_e = (short) 23;
  public static final Set<PropertyValue> SET_VAL_f = new HashSet<>();
  public static final Comparator<Element> ID_COMPARATOR = Comparator.comparing(Element::getId);
  public static final Function<String, AggregateFunction> STRING_CONC_FUNC = (key) -> new BaseAggregateFunction() {
    @Override
    public PropertyValue aggregate(PropertyValue aggregate, PropertyValue increment) {
      return PropertyValue.create(aggregate.getString() + increment.getString());
    }

    @Override
    public String getAggregatePropertyKey() {
      return key;
    }

    @Override
    public PropertyValue getIncrement(Element element) {
      return element.getPropertyValue(key);
    }
  };
  public static final Function<String, AggregateFunction> INT_ADD_FUNC = (key) -> new BaseAggregateFunction() {
    @Override
    public PropertyValue aggregate(PropertyValue aggregate, PropertyValue increment) {
      return PropertyValueUtils.Numeric.add(aggregate, increment);
    }

    @Override
    public String getAggregatePropertyKey() {
      return key;
    }

    @Override
    public PropertyValue getIncrement(Element element) {
      return element.getPropertyValue(key);
    }
  };
  private static final Comparator<Triplet> tripletComparator = (t1, t2) -> {
    int difference;

    // compare edges
    difference = compareGraphElementsViaProperties(t1.getEdge(), t2.getEdge());
    if (difference != 0) {
      return difference;
    }

    // compare source vertex
    difference = compareGraphElementsViaProperties(t1.getSourceVertex(), t2.getSourceVertex());
    if (difference != 0) {
      return difference;
    }

    // compare target vertex
    difference = compareGraphElementsViaProperties(t1.getTargetVertex(), t2.getTargetVertex());
    return difference;
  };
  /**
   * Contains values of all supported property types
   */
  public static Map<String, Object> SUPPORTED_PROPERTIES;

  static {
    MAP_VAL_9.put(PropertyValue.create(KEY_0), PropertyValue.create(NULL_VAL_0));
    MAP_VAL_9.put(PropertyValue.create(KEY_1), PropertyValue.create(BOOL_VAL_1));
    MAP_VAL_9.put(PropertyValue.create(KEY_2), PropertyValue.create(INT_VAL_2));
    MAP_VAL_9.put(PropertyValue.create(KEY_3), PropertyValue.create(LONG_VAL_3));
    MAP_VAL_9.put(PropertyValue.create(KEY_4), PropertyValue.create(FLOAT_VAL_4));
    MAP_VAL_9.put(PropertyValue.create(KEY_5), PropertyValue.create(DOUBLE_VAL_5));
    MAP_VAL_9.put(PropertyValue.create(KEY_6), PropertyValue.create(STRING_VAL_6));
    MAP_VAL_9.put(PropertyValue.create(KEY_7), PropertyValue.create(BIG_DECIMAL_VAL_7));
    MAP_VAL_9.put(PropertyValue.create(KEY_8), PropertyValue.create(GRADOOP_ID_VAL_8));
    MAP_VAL_9.put(PropertyValue.create(KEY_b), PropertyValue.create(DATE_VAL_b));
    MAP_VAL_9.put(PropertyValue.create(KEY_c), PropertyValue.create(TIME_VAL_c));
    MAP_VAL_9.put(PropertyValue.create(KEY_d), PropertyValue.create(DATETIME_VAL_d));
    MAP_VAL_9.put(PropertyValue.create(KEY_e), PropertyValue.create(SHORT_VAL_e));

    LIST_VAL_a.add(PropertyValue.create(NULL_VAL_0));
    LIST_VAL_a.add(PropertyValue.create(BOOL_VAL_1));
    LIST_VAL_a.add(PropertyValue.create(INT_VAL_2));
    LIST_VAL_a.add(PropertyValue.create(LONG_VAL_3));
    LIST_VAL_a.add(PropertyValue.create(FLOAT_VAL_4));
    LIST_VAL_a.add(PropertyValue.create(DOUBLE_VAL_5));
    LIST_VAL_a.add(PropertyValue.create(STRING_VAL_6));
    LIST_VAL_a.add(PropertyValue.create(BIG_DECIMAL_VAL_7));
    LIST_VAL_a.add(PropertyValue.create(GRADOOP_ID_VAL_8));
    LIST_VAL_a.add(PropertyValue.create(DATE_VAL_b));
    LIST_VAL_a.add(PropertyValue.create(TIME_VAL_c));
    LIST_VAL_a.add(PropertyValue.create(DATETIME_VAL_d));
    LIST_VAL_a.add(PropertyValue.create(SHORT_VAL_e));

    SET_VAL_f.add(PropertyValue.create(NULL_VAL_0));
    SET_VAL_f.add(PropertyValue.create(BOOL_VAL_1));
    SET_VAL_f.add(PropertyValue.create(INT_VAL_2));
    SET_VAL_f.add(PropertyValue.create(LONG_VAL_3));
    SET_VAL_f.add(PropertyValue.create(FLOAT_VAL_4));
    SET_VAL_f.add(PropertyValue.create(DOUBLE_VAL_5));
    SET_VAL_f.add(PropertyValue.create(STRING_VAL_6));
    SET_VAL_f.add(PropertyValue.create(BIG_DECIMAL_VAL_7));
    SET_VAL_f.add(PropertyValue.create(GRADOOP_ID_VAL_8));
    SET_VAL_f.add(PropertyValue.create(DATE_VAL_b));
    SET_VAL_f.add(PropertyValue.create(TIME_VAL_c));
    SET_VAL_f.add(PropertyValue.create(DATETIME_VAL_d));
    SET_VAL_f.add(PropertyValue.create(SHORT_VAL_e));

    SUPPORTED_PROPERTIES = Maps.newTreeMap();
    SUPPORTED_PROPERTIES.put(KEY_0, NULL_VAL_0);
    SUPPORTED_PROPERTIES.put(KEY_1, BOOL_VAL_1);
    SUPPORTED_PROPERTIES.put(KEY_2, INT_VAL_2);
    SUPPORTED_PROPERTIES.put(KEY_3, LONG_VAL_3);
    SUPPORTED_PROPERTIES.put(KEY_4, FLOAT_VAL_4);
    SUPPORTED_PROPERTIES.put(KEY_5, DOUBLE_VAL_5);
    SUPPORTED_PROPERTIES.put(KEY_6, STRING_VAL_6);
    SUPPORTED_PROPERTIES.put(KEY_7, BIG_DECIMAL_VAL_7);
    SUPPORTED_PROPERTIES.put(KEY_8, GRADOOP_ID_VAL_8);
    SUPPORTED_PROPERTIES.put(KEY_9, MAP_VAL_9);
    SUPPORTED_PROPERTIES.put(KEY_a, LIST_VAL_a);
    SUPPORTED_PROPERTIES.put(KEY_b, DATE_VAL_b);
    SUPPORTED_PROPERTIES.put(KEY_c, TIME_VAL_c);
    SUPPORTED_PROPERTIES.put(KEY_d, DATETIME_VAL_d);
    SUPPORTED_PROPERTIES.put(KEY_e, SHORT_VAL_e);
    SUPPORTED_PROPERTIES.put(KEY_f, SET_VAL_f);
  }

  /**
   * Creates a social network as a basis for tests.
   * <p/>
   * An image of the network can be found in gradoop/dev-support/social-network.pdf
   *
   * @return graph store containing a simple social network for tests.
   * @throws IOException on failure
   */
  public static AsciiGraphLoader getSocialNetworkLoader()
      throws IOException {
    InputStream inputStream = TestUtils.class.getResourceAsStream(SOCIAL_NETWORK_GDL_FILE);
    return AsciiGraphLoader.fromInputStream(inputStream);
  }

  /**
   * Checks if the two collections contain the same identifiers.
   *
   * @param expectedCollection first collection
   * @param actualCollection   second collection
   */
  public static void validateIdEquality(
      Collection<GradoopId> expectedCollection,
      Collection<GradoopId> actualCollection) {

    List<GradoopId> expectedList = new ArrayList<>(expectedCollection);
    List<GradoopId> actualList = new ArrayList<>(actualCollection);

    Collections.sort(expectedList);
    Collections.sort(actualList);

    assertThat(expectedList, hasSize(actualList.size()));
    Iterator<GradoopId> it1 = expectedList.iterator();
    Iterator<GradoopId> it2 = actualList.iterator();

    while (it1.hasNext() && it2.hasNext()) {
      assertEquals(it1.next(), it2.next(), "id mismatch");
    }
  }

  /**
   * Checks if no identifier is contained in both lists.
   *
   * @param originalCollection first collection
   * @param newCollection      second collection
   */
  public static void validateIdInequality(Collection<GradoopId> originalCollection,
      Collection<GradoopId> newCollection) {
    for (var originalId : originalCollection) {
      assertThat(newCollection, not(hasItem(originalId)));
    }
  }

  /**
   * Checks if two collections contain the same EPGM elements in terms of data (i.e. label and
   * properties).
   *
   * @param expectedCollection first collection
   * @param actualCollection   second collection
   */
  public static void validateElementCollections(
      Collection<? extends Element> expectedCollection,
      Collection<? extends Element> actualCollection) {
    assertNotNull(expectedCollection, "first collection was null");
    assertNotNull(expectedCollection, "second collection was null");
    assertEquals(expectedCollection.size(), actualCollection.size(), String.format(
        "collections of different size: %d and %d", expectedCollection.size(),
        actualCollection.size()));

    List<? extends Element> list1 = new ArrayList<>(expectedCollection);
    List<? extends Element> list2 = new ArrayList<>(actualCollection);

    list1.sort(ID_COMPARATOR);
    list2.sort(ID_COMPARATOR);

    Iterator<? extends Element> it1 = list1.iterator();
    Iterator<? extends Element> it2 = list2.iterator();

    while (it1.hasNext()) {
      validateElementWith(
          it1.next(),
          it2.next());
    }
  }

  /**
   * Sorts the given collections by element id and checks pairwise if elements are contained in the
   * same graphs.
   *
   * @param expectedCollection first collection
   * @param actualCollection   second collection
   */
  public static void validateGraphElementCollections(
      Collection<? extends GraphElement> expectedCollection,
      Collection<? extends GraphElement> actualCollection) {
    assertNotNull(expectedCollection, "first collection was null");
    assertNotNull(expectedCollection, "second collection was null");
    assertEquals(expectedCollection.size(), actualCollection.size(), String.format(
        "collections of different size: %d and %d", expectedCollection.size(),
        actualCollection.size()));

    List<? extends GraphElement> list1 = new ArrayList<>(expectedCollection);
    List<? extends GraphElement> list2 = new ArrayList<>(actualCollection);

    list1.sort(ID_COMPARATOR);
    list2.sort(ID_COMPARATOR);

    Iterator<? extends GraphElement> it1 = list1.iterator();
    Iterator<? extends GraphElement> it2 = list2.iterator();

    while (it1.hasNext()) {
      validateElementWith(it1.next(), it2.next());
    }
  }

  /**
   * Sorts the given collections by edge id and checks pairwise if the triplets are contained
   * in the same graphs.
   *
   * @param expectedCollection first collection
   * @param actualCollection   second collection
   */
  public static void validateTripletCollections(
      Collection<Triplet> expectedCollection,
      Collection<Triplet> actualCollection) {
    assertNotNull(expectedCollection, "first collection was null");
    assertNotNull(expectedCollection, "second collection was null");
    assertEquals(expectedCollection.size(), actualCollection.size(), String.format(
        "collections of different size: %d and %d", expectedCollection.size(),
        actualCollection.size()));

    var list1 = new ArrayList<>(expectedCollection);
    var list2 = new ArrayList<>(actualCollection);

    list1.sort(tripletComparator);
    list2.sort(tripletComparator);

    var it1 = list1.iterator();
    var it2 = list2.iterator();

    while (it1.hasNext()) {
      var triplet1 = it1.next();
      var triplet2 = it2.next();

      validateElementWith(triplet1.getEdge(), triplet2.getEdge());
      validateElementWith(triplet1.getSourceVertex(), triplet2.getSourceVertex());
      validateElementWith(triplet1.getTargetVertex(), triplet2.getTargetVertex());
    }
  }

  public static int compareGraphElementsViaProperties(GraphElement elem1, GraphElement elem2) {
    Comparator<GraphElement> elementComparator;
    int difference;

    elementComparator = Comparator.comparing(Element::getLabel);
    if ((difference = elementComparator.compare(elem1, elem2)) != 0) {
      return difference;
    }
    var unsortedPropKeys = elem1.getPropertyKeys();
    unsortedPropKeys = unsortedPropKeys != null ? unsortedPropKeys : Set.of();
    var sortedPropKeys = new PriorityQueue<String>();
    for (var keys : unsortedPropKeys) {
      sortedPropKeys.add(keys);
    }
    for (var key : sortedPropKeys) {
      if (elem2.hasProperty(key)) {
        var prop2 = elem2.getPropertyValue(key);
        if ((difference = elem1.getPropertyValue(key).compareTo(prop2)) != 0) {
          return difference;
        }
      } else {
        return -1;
      }
    }
    unsortedPropKeys = elem2.getPropertyKeys();
    unsortedPropKeys = unsortedPropKeys != null ? unsortedPropKeys : Set.of();
    sortedPropKeys = new PriorityQueue<>();
    for (var keys : unsortedPropKeys) {
      sortedPropKeys.add(keys);
    }
    for (var key : sortedPropKeys) {
      if (elem1.hasProperty(key)) {
        var prop2 = elem1.getPropertyValue(key);
        if ((difference = elem1.getPropertyValue(key).compareTo(prop2)) != 0) {
          return difference;
        }
      } else {
        return -1;
      }
    }
    elementComparator = Comparator
        .comparing(e -> e.getGraphIds().toString()); // TODO: better comparison for graphIds
    if ((difference = elementComparator.compare(elem1, elem2)) != 0) {
      return difference;
    }
    return 0;
  }


  public static void validateElementWith(Element expectedElement,
      Element actualElement) {
    assertNotNull(expectedElement, "expected element was null");
    assertNotNull(actualElement, "actual element was null");

    assertEquals(expectedElement.getLabel(), actualElement.getLabel(), "label mismatch");

    if (expectedElement.getPropertyCount() == 0) {
      assertEquals(expectedElement.getPropertyCount(), actualElement.getPropertyCount(),
          String.format("property count mismatch for %s & %s", expectedElement, actualElement));
    } else {
      List<String> expectedKeys = new ArrayList<>();
      expectedElement.getPropertyKeys().forEach(expectedKeys::add);
      List<String> actualKeys = new ArrayList<>();
      actualElement.getPropertyKeys().forEach(actualKeys::add);

      assertEquals(expectedKeys.size(), actualKeys.size(), String.format(
          "Expected element %s mismatched actual element %s. Number of property keys is different size: %d and %d",
          expectedElement, actualElement, expectedKeys.size(),
          actualKeys.size()));

      Collections.sort(expectedKeys);
      Collections.sort(actualKeys);

      var expectedKeyIt = expectedKeys.iterator();
      var actualKeyIt = actualKeys.iterator();

      while (expectedKeyIt.hasNext() && actualKeyIt.hasNext()) {
        String expectedKey = expectedKeyIt.next();
        String actualKey = actualKeyIt.next();
        assertEquals(expectedKey, actualKey,
            String.format("Expected element %s mismatched actual element %s."
                + "Property key mismatch", expectedElement, actualElement));
        assertEquals(expectedElement.getPropertyValue(expectedKey),
            actualElement.getPropertyValue(actualKey),
            String.format("Expected element %s mismatched actual element %s."
                    + "Property value mismatch on property key '%s'.",
                expectedElement, actualElement, actualKey));
      }
    }
  }

  /**
   * Uses reflection to call a private method with no arguments.
   *
   * @param clazz      class which has the method
   * @param object     instance of the class
   * @param methodName method name
   * @param <T1>       return type of method
   * @param <T2>       type of the calling class
   * @return method result
   * @throws Exception in case anything goes wrong
   */
  public static <T1, T2> T1 call(Class<T2> clazz, T2 object, String methodName)
      throws Exception {
    return call(clazz, object, methodName, null, null);
  }

  /**
   * Uses reflection to call a private method with arguments.
   *
   * @param clazz      class which has the method
   * @param object     instance of the class
   * @param methodName method name
   * @param args       method arguments
   * @param <T1>       return type of method
   * @param <T2>       type of the calling class
   * @return method result
   * @throws Exception in case anything goes wrong
   */
  @SuppressWarnings("unchecked")
  public static <T1, T2> T1 call(
      Class<T2> clazz,
      T2 object,
      String methodName,
      Class<?>[] parameterTypes, Object[] args)
      throws Exception {
    Method m = parameterTypes != null ?
        clazz.getDeclaredMethod(methodName, parameterTypes) : clazz.getDeclaredMethod(methodName);
    m.setAccessible(true);
    return (T1) (args != null ? m.invoke(object, args) : m.invoke(object));
  }

  /**
   * Uses reflection to call a private void method with arguments.
   *
   * @param clazz      class which has the method
   * @param object     instance of the class
   * @param methodName method name
   * @param args       method arguments
   * @param <Type>     type of the calling class
   * @throws Exception in case anything goes wrong
   */
  @SuppressWarnings("unchecked")
  public static <Type> void callVoid(
      Class<Type> clazz,
      Type object,
      String methodName,
      Class<?>[] parameterTypes, Object[] args)
      throws Exception {
    Method m = parameterTypes != null ?
        clazz.getDeclaredMethod(methodName, parameterTypes) : clazz.getDeclaredMethod(methodName);
    m.setAccessible(true);
    if (args != null) {
      m.invoke(object, args);
    } else {
      m.invoke(object);
    }
  }

  public static <T extends Serializable> byte[] pickle(T obj)
      throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(obj);
    oos.close();
    return baos.toByteArray();
  }

  public static <T extends Serializable> T unpickle(byte[] b, Class<T> cl)
      throws IOException, ClassNotFoundException {
    ByteArrayInputStream bais = new ByteArrayInputStream(b);
    ObjectInputStream ois = new ObjectInputStream(bais);
    Object o = ois.readObject();
    return cl.cast(o);
  }

  public static void assertThatStreamContains(BaseStream actualResultStream,
      Collection<Triplet> expectedResult)
      throws IOException {
    var tripletIt = actualResultStream.collect();
    var actualTriplets = new ArrayList<Triplet>();
    tripletIt.forEachRemaining(actualTriplets::add);
    assertThat(actualTriplets, containsInAnyOrder(expectedResult.toArray()));
  }
}

