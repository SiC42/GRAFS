package edu.leipzig.grafs.operators.grouping.model;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import edu.leipzig.grafs.util.TestUtils;
import java.util.Set;
import org.junit.jupiter.api.Test;

public class GroupingInformationTest {

  @Test
  public void testGroupingInformation_emptyConstructor(){
    var gi = new GroupingInformation();
    assertFalse(gi.shouldUseLabel());
    assertFalse(gi.shouldUseMembership());
    assertThat(gi.getKeys(), empty());
  }

  @Test
  public void testGroupingInformation_withKeySet(){
    var groupingKeys = Set.of(TestUtils.KEY_1, TestUtils.KEY_2);
    var gi = new GroupingInformation(groupingKeys);
    assertFalse(gi.shouldUseLabel());
    assertFalse(gi.shouldUseMembership());
    assertThat(gi.getKeys(), is(equalTo(groupingKeys)));
  }

  @Test
  public void testGroupingInformation_withValues(){
    var useLabel = true;
    var useMemberShip = true;
    var groupingKeys = Set.of(TestUtils.KEY_1, TestUtils.KEY_2);
    var gi = new GroupingInformation(useLabel, useMemberShip, groupingKeys);
    assertThat(gi.shouldUseLabel(), is(equalTo(useLabel)));
    assertThat(gi.shouldUseMembership(), is(equalTo(useMemberShip)));
    assertThat(gi.getKeys(), is(equalTo(groupingKeys)));
  }

  @Test
  public void testAddKey(){
    var gi = new GroupingInformation();
    gi.addKey("1");
    assertThat(gi.getKeys(), is(equalTo(Set.of("1"))));
  }

  @Test
  public void testAddKey_twoOfSameKey(){
    var gi = new GroupingInformation();
    gi.addKey("1");
    gi.addKey("1");
    assertThat(gi.getKeys(), is(equalTo(Set.of("1"))));
  }

  @Test
  public void testShouldUseLabel(){
    var gi = new GroupingInformation();
    gi.useLabel(true);
    assertTrue(gi.shouldUseLabel());
    gi.useLabel(false);
    assertFalse(gi.shouldUseLabel());
  }

  @Test
  public void testShouldUseMembership(){
    var gi = new GroupingInformation();
    gi.useMembership(true);
    assertTrue(gi.shouldUseMembership());
    gi.useMembership(false);
    assertFalse(gi.shouldUseMembership());
  }

}