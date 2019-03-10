package com.obsidiandynamics.runway.capacitor;

import static org.junit.Assert.*;

import java.util.*;

import org.junit.*;

import com.obsidiandynamics.verifier.*;

import nl.jqno.equalsverifier.*;

public final class ReclaimStatsTest {
  @Test
  public void testEmpty() {
    final var stats = ReclaimStats.empty();
    assertSame(stats, ReclaimStats.empty());
    assertEquals(0, stats.getReclaimedSpace());
    assertEquals(Collections.emptyList(), stats.getPurgedIds());
  }
  
  @Test
  public void testPojo() {
    PojoVerifier.forClass(ReclaimStats.class).excludeToStringField("purgedIds").verify();
  }
  
  @Test
  public void testEqualsHashCode() {
    EqualsVerifier.forClass(ReclaimStats.class).verify();
  }
}
