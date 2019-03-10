package com.obsidiandynamics.runway.capacitor;

import static java.util.Arrays.*;
import static org.junit.Assert.*;

import java.util.*;

import org.junit.*;

import com.obsidiandynamics.assertion.*;
import com.obsidiandynamics.func.*;
import com.obsidiandynamics.junit.*;
import com.obsidiandynamics.threads.*;

public final class HeapCacheTest {
  @Test
  public void testPutAndSizeAndPop() {
    final var cache = new HeapCache<Integer, Integer>()
        .withLocalTtl(60_000);
    cache.put(0, 0, 10);
    cache.put(1, 1, 20);
    
    final var size = cache.localSize();
    assertEquals(30, size.getTotalSize());
    assertEquals(MapBuilder.init(0, 10L).with(1, 20L).build(), size.getIndividualStats());
        
    assertEquals(0, (int) cache.pop(0));
    assertEquals(1, (int) cache.pop(1));
    
    assertEquals(SizeStats.empty(), cache.localSize());
    
    assertNull(cache.pop(0));
    assertNull(cache.pop(1));
  }
  
  @Test
  public void testNextTier() {
    final var nextTier = new HeapCache<Integer, Integer>();
    final var cache = new HeapCache<Integer, Integer>().withNextTier(nextTier);
    assertSame(nextTier, cache.nextTier());
  }
  
  @Test
  public void testReclaimUnderCapacityWithinTtl() {
    final var cache = new HeapCache<Integer, Integer>()
        .withLocalTtl(60_000);
    cache.put(0, 0, 10);
    cache.put(1, 1, 20);
    
    assertEquals(ReclaimStats.empty(), cache.reclaimLocal());
    
    assertNotNull(cache.pop(0));
    assertNotNull(cache.pop(1));
  }
  
  @Test
  public void testReclaimUnderCapacityOutsideTtl() {
    final var cache = new HeapCache<Integer, Integer>()
        .withLocalTtl(0);
    cache.put(0, 0, 10);
    cache.put(1, 1, 20);
    
    Threads.sleep(10);
    
    final var reclaim = cache.reclaimLocal();
    assertEquals(30, reclaim.getReclaimedSpace());
    assertEquals(new HashSet<>(asList(0, 1)), new HashSet<>(reclaim.getPurgedIds()));
    
    assertNull(cache.pop(0));
    assertNull(cache.pop(1));
  }
  
  @Test
  public void testReclaimOverCapacityWithinTtl() {
    final var cache = new HeapCache<Integer, Integer>()
        .withLocalTtl(60_000)
        .withLocalCapacity(0);
    cache.put(0, 0, 10);
    cache.put(1, 1, 20);
    
    Threads.sleep(10);
    
    final var reclaim = cache.reclaimLocal();
    assertEquals(30, reclaim.getReclaimedSpace());
    assertEquals(new HashSet<>(asList(0, 1)), new HashSet<>(reclaim.getPurgedIds()));
    
    assertNull(cache.pop(0));
    assertNull(cache.pop(1));
  }
  
  @Test
  public void testReclaimWithinTtlDownToJustUnderCapacity() {
    final var cache = new HeapCache<Integer, Integer>()
        .withLocalTtl(60_000)
        .withLocalCapacity(20);
    cache.put(0, 0, 10);
    cache.put(1, 1, 10);
    cache.put(2, 2, 10);
    
    Threads.sleep(10);
    assertEquals(30, cache.localSize().getTotalSize());
    
    final var reclaim = cache.reclaimLocal();
    assertEquals(10, reclaim.getReclaimedSpace());
    assertThat(reclaim.getPurgedIds(), HamcrestMatchers.fulfils(purgedIds -> {
      return purgedIds.contains(0) || purgedIds.contains(1) || purgedIds.contains(2);
    }));
  }
  
  @Test
  public void testReclaimToNextTier() {
    final var cache = new HeapCache<Integer, Integer>()
        .withLocalCapacity(0)
        .withNextTier(new HeapCache<>());

    cache.put(0, 0, 10);
    cache.put(1, 1, 20);
    
    final var reclaim = cache.reclaimLocal();
    assertEquals(30, reclaim.getReclaimedSpace());
    assertEquals(new HashSet<>(asList(0, 1)), new HashSet<>(reclaim.getPurgedIds()));
    
    final var nextSize = cache.nextTier().localSize();
    assertEquals(30, nextSize.getTotalSize());
    assertEquals(MapBuilder.init(0, 10L).with(1, 20L).build(), nextSize.getIndividualStats());
  }
  
  @Test
  public void testPopMissWithNextTierHit() {
    final var cache = new HeapCache<Integer, Integer>()
        .withLocalCapacity(0)
        .withNextTier(new HeapCache<>());

    cache.nextTier().put(0, 0, 10);
    cache.nextTier().put(1, 1, 20);
    
    assertEquals(SizeStats.empty(), cache.localSize());
    
    assertEquals(0, (int) cache.pop(0));
    assertEquals(1, (int) cache.pop(1));
    
    assertEquals(SizeStats.empty(), cache.localSize());
    assertEquals(SizeStats.empty(), cache.nextTier().localSize());
    
    assertNull(cache.pop(0));
    assertNull(cache.pop(1));
  }
  
  @Test
  public void testPopMissWithNextTierMiss() {
    final var cache = new HeapCache<Integer, Integer>()
        .withLocalCapacity(0)
        .withNextTier(new HeapCache<>());

    assertEquals(SizeStats.empty(), cache.localSize());
    
    assertNull(cache.pop(0));
  }
  
  @Test
  public void testClear() {
    final var cache = new HeapCache<Integer, Integer>()
        .withLocalCapacity(0)
        .withNextTier(new HeapCache<>());
    cache.put(0, 0, 1);
    cache.put(1, 1, 2);
    cache.nextTier().put(10, 10, 10);
    cache.nextTier().put(11, 11, 20);
    assertEquals(3, cache.localSize().getTotalSize());
    assertEquals(30, cache.nextTier().localSize().getTotalSize());
    
    cache.clear();
    assertEquals(0, cache.localSize().getTotalSize());
    assertEquals(0, cache.nextTier().localSize().getTotalSize());
  }
  
  @Test
  public void testToString() {
    Assertions.assertToStringOverride(new HeapCache<>());
  }
}
