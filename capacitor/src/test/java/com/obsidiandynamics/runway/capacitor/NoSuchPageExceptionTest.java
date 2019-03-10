package com.obsidiandynamics.runway.capacitor;

import static org.junit.Assert.*;

import org.junit.*;

public final class NoSuchPageExceptionTest {
  @Test
  public void testConstructor() {
    assertEquals("someError", new NoSuchPageException("someError").getMessage());
  }
}
