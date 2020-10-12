package edu.leipzig.grafs.benchmark;

import static org.junit.jupiter.api.Assertions.assertNotEquals;

import edu.leipzig.grafs.model.Element;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import org.gradoop.common.model.impl.properties.Properties;

public abstract class TransformationBaseTest extends OperatorTestBase {

  static void validateElementsDifference(Element originalElement,
      Element actualElement) {

    Properties originalProperties = Properties.create();
    originalElement.getProperties().forEach(originalProperties::set);
    Properties actualProperties = Properties.create();
    actualElement.getProperties().forEach(actualProperties::set);

    for (var originalProp : originalProperties) {
      var originalKey = originalProp.getKey();
      if (actualProperties.containsKey(originalKey)) {
        assertNotEquals(originalProp, actualProperties.get(originalKey));
        actualProperties.remove(originalKey);
      }
    }
    for (var actualProp : actualProperties) {
      var actualKey = actualProp.getKey();
      if (actualProperties.containsKey(actualKey)) {
        assertNotEquals(originalProperties.get(actualKey), actualProp);
      }
    }
  }

  /**
   * Checks if two collections contain the same EPGM elements in terms of data (i.e. label and
   * properties).
   *
   * @param expectedCollection first collection
   * @param actualCollection   second collection
   */
  protected void validateDataInequality(Collection<? extends Element> expectedCollection,
      Collection<? extends Element> actualCollection) {

    List<? extends Element> list1 = new ArrayList<>(expectedCollection);
    List<? extends Element> list2 = new ArrayList<>(actualCollection);
    final Comparator<Element> ID_COMPARATOR = Comparator.comparing(Element::getId);
    list1.sort(ID_COMPARATOR);
    list2.sort(ID_COMPARATOR);

    Iterator<? extends Element> it1 = list1.iterator();
    Iterator<? extends Element> it2 = list2.iterator();

    while (it1.hasNext()) {
      validateElementsDifference(
          it1.next(),
          it2.next());
    }
  }

}
