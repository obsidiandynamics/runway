package com.obsidiandynamics.runway.capacitor;

import static org.mockito.Mockito.*;

import org.junit.*;
import org.mockito.*;

public final class PageAccessTest {
  @SuppressWarnings("unchecked")
  @Test(expected=UnsupportedOperationException.class)
  public void testNonCachingDigest() {
    final var access = mock(PageAccess.NonCaching.class, Answers.CALLS_REAL_METHODS);
    access.digestOf(null);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testUnloadObjectDefaultImpl() {
    final var access = mock(PageAccess.NonCaching.class, Answers.CALLS_REAL_METHODS);
    access.unload(null, null);
  }
}
