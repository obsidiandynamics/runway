
package com.obsidiandynamics.runway.capacitor;

import static org.junit.Assert.*;

import java.util.*;

import org.junit.*;

import com.obsidiandynamics.verifier.*;

import nl.jqno.equalsverifier.*;

public final class SizeStatsTest {
  @Test
  public void testEmpty() {
    final var stats = SizeStats.empty();
    assertSame(stats, SizeStats.empty());
    assertEquals(0, stats.getTotalSize());
    assertEquals(Collections.emptyMap(), stats.getIndividualStats());
  }
  
  @Test
  public void testPojo() {
    PojoVerifier.forClass(SizeStats.class).excludeToStringField("individualStats").verify();
  }
  
  @Test
  public void testEqualsHashCode() {
    EqualsVerifier.forClass(SizeStats.class).verify();
  }
}
