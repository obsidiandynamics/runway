package com.obsidiandynamics.runway.capacitor;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.junit.*;
import org.mockito.*;

import com.obsidiandynamics.runway.capacitor.TieredCache.*;

public final class TieredCacheTest {
  @Test
  public void testNonTerminal() {
    final var cache = mock(NonTerminal.class, Answers.CALLS_REAL_METHODS);
    assertTrue(cache.hasNextTier());
  }
}
