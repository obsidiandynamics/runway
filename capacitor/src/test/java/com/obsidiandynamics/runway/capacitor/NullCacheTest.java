package com.obsidiandynamics.runway.capacitor;

import static org.junit.Assert.*;

import java.util.*;

import org.junit.*;

public final class NullCacheTest {
  @Test
  public void testMethods() {
    final var cache = NullCache.getInstance();
    assertSame(cache, NullCache.getInstance());

    assertNull(cache.pop(0));
    cache.put(0, "value", 100);
    assertFalse(cache.hasNextTier());

    final var size = cache.localSize();
    assertEquals(0, size.getTotalSize());
    assertEquals(Collections.emptyMap(), size.getIndividualStats());

    final var reclaim = cache.reclaimLocal();
    assertEquals(0, reclaim.getReclaimedSpace());
    assertEquals(Collections.emptyList(), reclaim.getPurgedIds());
    
    cache.clear();
  }

  @Test(expected=UnsupportedOperationException.class)
  public void testTerminalTier() {
    NullCache.getInstance().nextTier();
  }
}
