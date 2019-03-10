package com.obsidiandynamics.runway.capacitor;

import static org.junit.Assert.*;

import java.io.*;
import java.net.*;

import org.junit.*;

import com.obsidiandynamics.io.*;
import com.obsidiandynamics.verifier.*;
import com.obsidiandynamics.yconf.*;

public final class CapacitorConfigTest {
  @Test
  public void testPojo() {
    PojoVerifier.forClass(CapacitorConfig.class)
    .excludeMutators()
    .verify();
  }
  
  @Test
  public void testFluent() {
    FluentVerifier.forClass(CapacitorConfig.class).verify();
  }
  
  @Test
  public void testLoadFromConfig() throws FileNotFoundException, IOException {
    final var config = new MappingContext()
        .withParser(new SnakeyamlParser())
        .fromStream(ResourceLoader.stream(URI.create("cp://capacitor.yaml")))
        .map(CapacitorConfig.class);
    assertEquals(5_000, config.getResidentTtl());
    assertEquals(1_000, config.getResidentCapacity());
    assertEquals(10_000, config.getCacheTtl());
    assertEquals(2_000, config.getCacheCapacity());
  }
}
