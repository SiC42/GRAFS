package streaming.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;
import streaming.model.Element;
import streaming.model.GraphElement;


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
  public static final int INT_VAL_2 = 23;
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
    return AsciiGraphLoader.fromStream(inputStream);
  }

  /**
   * Checks if the two collections contain the same identifiers.
   *
   * @param collection1 first collection
   * @param collection2 second collection
   */
  public static void validateIdEquality(
      Collection<GradoopId> collection1,
      Collection<GradoopId> collection2) {

    List<GradoopId> list1 = Lists.newArrayList(collection1);
    List<GradoopId> list2 = Lists.newArrayList(collection2);

    Collections.sort(list1);
    Collections.sort(list2);
    Iterator<GradoopId> it1 = list1.iterator();
    Iterator<GradoopId> it2 = list2.iterator();

    while (it1.hasNext()) {
      assertEquals(it1.next(), it2.next(), "id mismatch");
    }
    assertFalse(it1.hasNext(), "too many elements in first collection");
    assertFalse(it2.hasNext(), "too many elements in second collection");
  }

  /**
   * Checks if no identifier is contained in both lists.
   *
   * @param collection1 first collection
   * @param collection2 second collection
   */
  public static void validateIdInequality(
      Collection<GradoopId> collection1,
      Collection<GradoopId> collection2) {

    for (GradoopId id1 : collection1) {
      for (GradoopId id2 : collection2) {
        assertNotEquals(id1, id2, "id in both collections");
      }
    }
  }


  /**
   * Checks if two given EPGM elements are equal by considering their label and properties.
   *
   * @param element1 first element
   * @param element2 second element
   */
  public static void validateElements(
      Element element1, Element element2) {
    assertNotNull(element1, "first element was null");
    assertNotNull(element2, "second element was null");

    assertEquals(element1.getId(), element2.getId(), "id mismatch");
    assertEquals("label mismatch", element1.getLabel(), element2.getLabel());

    if (element1.getPropertyCount() == 0) {
      assertEquals(element1.getPropertyCount(), element2.getPropertyCount(),
          "property count mismatch");
    } else {
      List<String> keys1 = Lists.newArrayList(element1.getPropertyKeys());
      Collections.sort(keys1);

      List<String> keys2 = Lists.newArrayList(element2.getPropertyKeys());
      Collections.sort(keys2);

      Iterator<String> it1 = keys1.iterator();
      Iterator<String> it2 = keys2.iterator();

      while (it1.hasNext() && it2.hasNext()) {
        String key1 = it1.next();
        String key2 = it2.next();
        assertEquals("property key mismatch", key1, key2);
        assertEquals(element1.getPropertyValue(key1), element2.getPropertyValue(key2),
            "property value mismatch");
      }
      assertFalse(it1.hasNext(), "too many properties in first element");
      assertFalse(it2.hasNext(), "too many properties in second element");
    }
  }

  /**
   * Checks if two given EPGM graph elements are equal by considering the graphs they are contained
   * in.
   *
   * @param element1 first element
   * @param element2 second element
   */
  public static void validateGraphElements(
      GraphElement element1, GraphElement element2) {

    assertNotNull(element1, "first element was null");
    assertNotNull(element2, "second element was null");

    assertTrue(element1.getGraphIds().equals(element2.getGraphIds()),
        String.format("graph containment mismatch. expected: %s actual: %s",
            element1.getGraphIds(), element2.getGraphIds())
    );
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
}

