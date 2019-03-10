package com.obsidiandynamics.runway.capacitor;

import org.junit.*;

public final class PageLoadExceptionTest {
  @Test
  public void testConstuctors() {
    new PageLoadException("message");
    new PageLoadException("message", new Throwable());
    new PageLoadException(new Throwable());
  }
}
