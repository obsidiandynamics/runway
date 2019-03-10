package com.obsidiandynamics.runway.capacitor;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.util.*;
import java.util.concurrent.atomic.*;

import org.junit.*;
import org.junit.runner.*;
import org.junit.runners.*;
import org.mockito.*;

import com.obsidiandynamics.runway.capacitor.CapacitorGC.*;
import com.obsidiandynamics.runway.capacitor.CapacitorTest.*;
import com.obsidiandynamics.await.*;
import com.obsidiandynamics.func.*;
import com.obsidiandynamics.junit.*;
import com.obsidiandynamics.threads.*;

/**
 *  Puts load on the capacitor to test for race conditions.
 */
@RunWith(Parameterized.class)
public final class CapacitorRaceTest {
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return TestCycle.timesQuietly(1);
  }
  
  private static final int RACE_SCALE = 10;

  private static final class TestPayloadAtomic implements Cloneable {
    final int id;
    final AtomicInteger amount = new AtomicInteger();
    long created = System.currentTimeMillis();
    TestPayloadAtomic(int id) { this.id = id; }
    
    @Override
    public TestPayloadAtomic clone() {
      final var clone = new TestPayloadAtomic(id);
      clone.created = created;
      clone.amount.set(amount.get());
      return clone;
    }
  }
  
  private CapacitorGC gc;
  
  @After
  public void after() {
    if (gc != null) {
      gc.terminate().joinSilently();
      gc = null;
    }
  }
  
  /**
   *  Multiple concurrent clients are contending for pages, with sufficient TTL and max capacity to avoid
   *  expiring pages. Tests that at most one instance of a page exists at any time.
   * 
   *  @throws PageLoadException 
   *  @throws NoSuchPageException 
   */
  @Test
  public void testRaceMultipleClientsWithoutExpiry() throws PageLoadException, NoSuchPageException {
    final var objects = 4;
    final var threads = 2 * Runtime.getRuntime().availableProcessors();
    final var runsPerThread = 10 * RACE_SCALE;
    
    final var persistedValues = new AtomicReferenceArray<TestPayload>(objects);
    final var accessType = Classes.<Class<PageAccess<Integer, TestPayload>>>cast(PageAccess.NonCaching.class);
    final var access = mock(accessType, Answers.CALLS_REAL_METHODS);
    final var loadCounts = new AtomicIntegerArray(objects);
    final var capacitor = new Capacitor<>(access)
        .withResidentTtl(60_000)
        .withResidentCapacity(Long.MAX_VALUE)
        .withCache(NullCache.getInstance());
    when(access.load(isNotNull(), any())).then(invocation -> {
      final var id = invocation.<Integer>getArgument(0);
      final var digest = invocation.<Integer>getArgument(1);
      final var loadCount = loadCounts.getAndIncrement(id);
      assertEquals(0, loadCount);
      persistedValues.compareAndSet(id, null, new TestPayload(id));
      final var persistedValue = persistedValues.get(id);
      if (digest != null && (int) digest == persistedValue.amount) {
        return null;
      } else {
        return persistedValue.clone();
      }
    });
    when(access.sizeOf(isNotNull())).then(invocation -> (long) invocation.<TestPayload>getArgument(0).id + 1);
    doAnswer(invocation -> {
      final var id = invocation.<Integer>getArgument(0);
      final var loadCount = loadCounts.getAndDecrement(id);
      assertEquals(1, loadCount);
      return null;
    }).when(access).unload(any(), isNotNull());
    
    final var error = new AtomicReference<Throwable>();
    Parallel.blocking(threads, __ -> {
      for (var run = 0; run < runsPerThread; run++) {
        for (var id = 0; id < objects; id++) {
          try (var handle = capacitor.acquire(id).lock(LockMode.EXCLUSIVE)) {
            final var value = handle.value();
            value.amount++;
            final var persistedValue = persistedValues.get(id);
            persistedValue.amount++;
          } catch (Throwable e) { 
            error.set(e);
          }
        }
      }
    }).run();
    
    if (error.get() != null) {
      throw new AssertionError("An error was caught", error.get());
    }
    
    for (var id = 0; id < objects; id++) {
      // each item should be loaded exactly once
      assertEquals(1, loadCounts.get(id));
      // the amount in the persisted value should equate to the number of invocations across the contending threads
      assertEquals(threads * runsPerThread, persistedValues.get(id).amount);
      // the most recent page should line up with the persisted value
      try (var handle = capacitor.acquire(id)) {
        assertEquals(threads * runsPerThread, handle.value().amount);
      }
    }

    verify(access, times(objects)).load(isNotNull(), isNull());
    verify(access, never()).load(isNotNull(), isNotNull());
    verify(access, never()).sizeOf(any());
    verify(access, never()).digestOf(isNotNull());
    verify(access, never()).unload(any(), any());
  }

  /**
   *  Same as {@link #testRaceMultipleClientsWithoutExpiry()}, but acquires the page without any locks, allowing
   *  for concurrent mutations of both the page and persisted state object. {@link TestPayloadAtomic} is used
   *  for thread safety.
   *  
   *  @throws PageLoadException
   *  @throws NoSuchPageException 
   */
  @Test
  public void testRaceMultipleClientsWithoutExpiryNoLock() throws PageLoadException, NoSuchPageException {
    final var objects = 4;
    final var threads = 2 * Runtime.getRuntime().availableProcessors();
    final var runsPerThread = 10 * RACE_SCALE;
    
    final var persistedValues = new AtomicReferenceArray<TestPayloadAtomic>(objects);
    final var accessType = Classes.<Class<PageAccess<Integer, TestPayloadAtomic>>>cast(PageAccess.NonCaching.class);
    final var access = mock(accessType, Answers.CALLS_REAL_METHODS);
    final var loadCounts = new AtomicIntegerArray(objects);
    final var capacitor = new Capacitor<>(access)
        .withResidentTtl(60_000)
        .withResidentCapacity(Long.MAX_VALUE)
        .withCache(NullCache.getInstance());
    when(access.load(isNotNull(), any())).then(invocation -> {
      final var id = invocation.<Integer>getArgument(0);
      final var digest = invocation.<Integer>getArgument(1);
      final var loadCount = loadCounts.getAndIncrement(id);
      assertEquals(0, loadCount);
      persistedValues.compareAndSet(id, null, new TestPayloadAtomic(id));
      final var persistedValue = persistedValues.get(id);
      if (digest != null && (int) digest == persistedValue.amount.get()) {
        return null;
      } else {
        return persistedValue.clone();
      }
    });
    when(access.sizeOf(isNotNull())).then(invocation -> (long) invocation.<TestPayloadAtomic>getArgument(0).id + 1);
    doAnswer(invocation -> {
      final var id = invocation.<Integer>getArgument(0);
      final var loadCount = loadCounts.getAndDecrement(id);
      assertEquals(1, loadCount);
      return null;
    }).when(access).unload(any(), isNotNull());
    
    final var error = new AtomicReference<Throwable>();
    Parallel.blocking(threads, __ -> {
      for (var run = 0; run < runsPerThread; run++) {
        for (var id = 0; id < objects; id++) {
          try (var handle = capacitor.acquire(id)) {
            final var value = handle.value();
            value.amount.incrementAndGet();
            final var persistedValue = persistedValues.get(id);
            persistedValue.amount.incrementAndGet();
          } catch (Throwable e) { 
            error.set(e);
          }
        }
      }
    }).run();
    
    if (error.get() != null) {
      throw new AssertionError("An error was caught", error.get());
    }
    
    for (var id = 0; id < objects; id++) {
      // each item should be loaded exactly once
      assertEquals(1, loadCounts.get(id));
      // the amount in the persisted value should equate to the number of invocations across the contending threads
      assertEquals(threads * runsPerThread, persistedValues.get(id).amount.get());
      // the most recent page should line up with the persisted value
      try (var handle = capacitor.acquire(id)) {
        assertEquals(threads * runsPerThread, handle.value().amount.get());
      }
    }

    verify(access, times(objects)).load(isNotNull(), isNull());
    verify(access, never()).load(isNotNull(), isNotNull());
    verify(access, never()).sizeOf(any());
    verify(access, never()).digestOf(isNotNull());
    verify(access, never()).unload(any(), isNotNull());
  }
  
  /**
   *  Multiple concurrent clients with continuous expiry of pages in the absence of any caching. Tests that at most 
   *  one instance of a page exists at any time.
   * 
   *  @throws PageLoadException 
   *  @throws NoSuchPageException 
   */
  @Test
  public void testRaceMultipleClientsWithExpiry() throws PageLoadException, NoSuchPageException {
    final var objects = 4;
    final var threads = 2 * Runtime.getRuntime().availableProcessors();
    final var runsPerThread = 10 * RACE_SCALE;
    
    final var persistedValues = new AtomicReferenceArray<TestPayload>(objects);
    final var access = Classes.<PageAccess<Integer, TestPayload>>cast(mock(PageAccess.NonCaching.class, Answers.CALLS_REAL_METHODS));
    final var loadCounts = new AtomicIntegerArray(objects);
    final var loadHits = new AtomicIntegerArray(objects);
    final var loadMisses = new AtomicIntegerArray(objects);
    final var capacitor = new Capacitor<>(access)
        .withResidentTtl(0)
        .withResidentCapacity(0)
        .withCache(NullCache.getInstance());
    when(access.load(isNotNull(), any())).then(invocation -> {
      final var id = invocation.<Integer>getArgument(0);
      final var digest = invocation.<Integer>getArgument(1);
      final var loadCount = loadCounts.getAndIncrement(id);
      assertEquals(0, loadCount);
      persistedValues.compareAndSet(id, null, new TestPayload(id));
      final var persistedValue = persistedValues.get(id);
      if (digest != null && (int) digest == persistedValue.amount) {
        loadHits.incrementAndGet(id);
        return null;
      } else {
        loadMisses.incrementAndGet(id);
        return persistedValue.clone();
      }
    });
    when(access.sizeOf(isNotNull())).then(invocation -> (long) invocation.<TestPayload>getArgument(0).id + 1);
    doAnswer(invocation -> {
      final var id = invocation.<Integer>getArgument(0);
      final var loadCount = loadCounts.getAndDecrement(id);
      assertEquals(1, loadCount);
      return null;
    }).when(access).unload(any(), isNotNull());
    
    final var error = new AtomicReference<Throwable>();
    Parallel.blocking(threads, __ -> {
      for (var run = 0; run < runsPerThread; run++) {
        for (var id = 0; id < objects; id++) {
          try (var handle = capacitor.acquire(id).lock(LockMode.EXCLUSIVE)) {
            final var value = handle.value();
            value.amount++;
            final var persistedValue = persistedValues.get(id);
            persistedValue.amount++;
          } catch (Throwable e) { 
            error.set(e);
          }
        }
      }
    }).run();
    
    if (error.get() != null) {
      throw new AssertionError("An error was caught", error.get());
    }

    for (var id = 0; id < objects; id++) {
      // each item should be loaded exactly once (load counts == unload counts - 1)
      assertEquals(1, loadCounts.get(id));
      // the amount in the persisted value should equate to the number of invocations across the contending threads
      assertEquals(threads * runsPerThread, persistedValues.get(id).amount);
      // the most recent page should line up with the persisted value
      try (var handle = capacitor.acquire(id)) {
        assertEquals(threads * runsPerThread, handle.value().amount);
      }
      assertEquals(0, loadHits.get(id));
      assertTrue("loadMisses=" + loadMisses, loadMisses.get(id) >= 1);
    }

    verify(access, atLeast(objects)).load(isNotNull(), isNull());
    verify(access, never()).load(isNotNull(), isNotNull());
    verify(access, never()).digestOf(isNotNull());
  }

  /**
   *  Same as {@link #testRaceMultipleClientsWithExpiry()}, but uses a shared lock, allowing
   *  for concurrent mutations of both the page and persisted state object. {@link TestPayloadAtomic} is used
   *  for thread safety.
   *  
   *  @throws PageLoadException
   *  @throws NoSuchPageException 
   */
  @Test
  public void testRaceMultipleClientsWithExpirySharedLock() throws PageLoadException, NoSuchPageException {
    final var objects = 4;
    final var threads = 2 * Runtime.getRuntime().availableProcessors();
    final var runsPerThread = 10 * RACE_SCALE;
    
    final var persistedValues = new AtomicReferenceArray<TestPayloadAtomic>(objects);
    final var access = Classes.<PageAccess<Integer, TestPayloadAtomic>>cast(mock(PageAccess.NonCaching.class, Answers.CALLS_REAL_METHODS));
    final var loadCounts = new AtomicIntegerArray(objects);
    final var loadHits = new AtomicIntegerArray(objects);
    final var loadMisses = new AtomicIntegerArray(objects);
    final var capacitor = new Capacitor<>(access)
        .withResidentTtl(0)
        .withResidentCapacity(0)
        .withCache(NullCache.getInstance());
    when(access.load(isNotNull(), any())).then(invocation -> {
      final var id = invocation.<Integer>getArgument(0);
      final var digest = invocation.<Integer>getArgument(1);
      final var loadCount = loadCounts.getAndIncrement(id);
      assertEquals(0, loadCount);
      persistedValues.compareAndSet(id, null, new TestPayloadAtomic(id));
      final var persistedValue = persistedValues.get(id);
      if (digest != null && (int) digest == persistedValue.amount.get()) {
        loadHits.incrementAndGet(id);
        return null;
      } else {
        loadMisses.incrementAndGet(id);
        return persistedValue.clone();
      }
    });
    when(access.sizeOf(isNotNull())).then(invocation -> (long) invocation.<TestPayloadAtomic>getArgument(0).id + 1);
    doAnswer(invocation -> {
      final var id = invocation.<Integer>getArgument(0);
      final var loadCount = loadCounts.getAndDecrement(id);
      assertEquals(1, loadCount);
      return null;
    }).when(access).unload(any(), isNotNull());
    
    final var error = new AtomicReference<Throwable>();
    Parallel.blocking(threads, __ -> {
      for (var run = 0; run < runsPerThread; run++) {
        for (var id = 0; id < objects; id++) {
          try (var handle = capacitor.acquire(id).lock(LockMode.SHARED)) {
            final var value = handle.value();
            value.amount.incrementAndGet();
            final var persistedValue = persistedValues.get(id);
            persistedValue.amount.incrementAndGet();
          } catch (Throwable e) { 
            error.set(e);
          }
        }
      }
    }).run();
    
    if (error.get() != null) {
      throw new AssertionError("An error was caught", error.get());
    }

    for (var id = 0; id < objects; id++) {
      // each item should be loaded exactly once (load counts == unload counts - 1)
      assertEquals(1, loadCounts.get(id));
      // the amount in the persisted value should equate to the number of invocations across the contending threads
      assertEquals(threads * runsPerThread, persistedValues.get(id).amount.get());
      // the most recent page should line up with the persisted value
      try (var handle = capacitor.acquire(id)) {
        assertEquals(threads * runsPerThread, handle.value().amount.get());
      }
      assertEquals(0, loadHits.get(id));
      assertTrue("loadMisses=" + loadMisses, loadMisses.get(id) >= 1);
    }

    verify(access, atLeast(objects)).load(isNotNull(), isNull());
    verify(access, never()).load(isNotNull(), isNotNull());
    verify(access, never()).digestOf(isNotNull());
  }
  
  /**
   *  Multiple concurrent clients with continuous expiry and infinite near-cache. Even though pages are
   *  regularly being reclaimed, the loading should always be from a cache (except the first load). At the
   *  end of the run, we verify that the page has only been loaded once and that the state of the paged 
   *  object is identical to the persistent object.
   * 
   *  @throws PageLoadException 
   *  @throws NoSuchPageException 
   */
  @Test
  public void testRaceMultipleClientsWithExpiryAndCaching() throws PageLoadException, NoSuchPageException {
    final var objects = 4;
    final var threads = 2 * Runtime.getRuntime().availableProcessors();
    final var runsPerThread = 10 * RACE_SCALE;
    
    final var persistedValues = new AtomicReferenceArray<TestPayload>(objects);
    final var access = Classes.<PageAccess<Integer, TestPayload>>cast(mock(PageAccess.Caching.class, Answers.CALLS_REAL_METHODS));
    final var loadCounts = new AtomicIntegerArray(objects);
    final var loadHits = new AtomicIntegerArray(objects);
    final var loadMisses = new AtomicIntegerArray(objects);
    final var capacitor = new Capacitor<>(access)
        .withResidentTtl(0)
        .withResidentCapacity(0)
        .withCache(new HeapCache<>());
    when(access.load(isNotNull(), any())).then(invocation -> {
      final var id = invocation.<Integer>getArgument(0);
      final var digest = invocation.<Integer>getArgument(1);
      final var loadCount = loadCounts.getAndIncrement(id);
      assertEquals(0, loadCount);
      persistedValues.compareAndSet(id, null, new TestPayload(id));
      final var persistedValue = persistedValues.get(id);
      if (digest != null && (int) digest == persistedValue.amount) {
        loadHits.incrementAndGet(id);
        return null;
      } else {
        if (digest != null) {
          // shouldn't happen, as all items can be popped out of the cache
          fail("Cache miss: digest=" + digest + ", persistedValue.amount=" + persistedValue.amount);
        }
        loadMisses.incrementAndGet(id);
        return persistedValue.clone();
      }
    });
    when(access.sizeOf(isNotNull())).then(invocation -> (long) invocation.<TestPayload>getArgument(0).id + 1);
    when(access.digestOf(isNotNull())).then(invocation -> (int) invocation.<TestPayload>getArgument(0).amount);
    doAnswer(invocation -> {
      final var id = invocation.<Integer>getArgument(0);
      final var loadCount = loadCounts.getAndDecrement(id);
      assertEquals(1, loadCount);
      return null;
    }).when(access).unload(any(), isNotNull());
    
    final var error = new AtomicReference<Throwable>();
    Parallel.blocking(threads, __ -> {
      for (var run = 0; run < runsPerThread; run++) {
        for (var id = 0; id < objects; id++) {
          try (var handle = capacitor.acquire(id).lock(LockMode.EXCLUSIVE)) {
            final var value = handle.value();
            value.amount++;
            final var persistedValue = persistedValues.get(id);
            persistedValue.amount++;
          } catch (Throwable e) { 
            error.set(e);
          }
        }
      }
    }).run();
    
    if (error.get() != null) {
      throw new AssertionError("An error was caught", error.get());
    }

    for (var id = 0; id < objects; id++) {
      // each item should be loaded exactly once (load counts == unload counts - 1)
      assertEquals(1, loadCounts.get(id));
      // the amount in the persisted value should equate to the number of invocations across the contending threads
      assertEquals(threads * runsPerThread, persistedValues.get(id).amount);
      // the most recent page should line up with the persisted value
      try (var handle = capacitor.acquire(id)) {
        assertEquals(threads * runsPerThread, handle.value().amount);
      }
      assertEquals(1, loadMisses.get(id));
    }
  }

  /**
   *  Same test as {@link #testRaceMultipleClientsWithExpiryAndCaching()}, but has a long TTL with zero capacity,
   *  so that objects are not expunged on load. Instead, GC is used to aggressively drive expiry.
   * 
   *  @throws PageLoadException 
   *  @throws NoSuchPageException 
   */
  @Test
  public void testRaceMultipleClientsWithExpiryAndCachingWithGC() throws PageLoadException, NoSuchPageException {
    final var objects = 4;
    final var threads = 2 * Runtime.getRuntime().availableProcessors();
    final var runsPerThread = 10 * RACE_SCALE;
    
    final var persistedValues = new AtomicReferenceArray<TestPayload>(objects);
    final var access = Classes.<PageAccess<Integer, TestPayload>>cast(mock(PageAccess.Caching.class, Answers.CALLS_REAL_METHODS));
    final var loadCounts = new AtomicIntegerArray(objects);
    final var loadHits = new AtomicIntegerArray(objects);
    final var loadMisses = new AtomicIntegerArray(objects);
    final var capacitor = new Capacitor<>(access)
        .withResidentTtl(60_000)
        .withResidentCapacity(0)
        .withCache(new HeapCache<>());
    when(access.load(isNotNull(), any())).then(invocation -> {
      final var id = invocation.<Integer>getArgument(0);
      final var digest = invocation.<Integer>getArgument(1);
      final var loadCount = loadCounts.getAndIncrement(id);
      assertEquals(0, loadCount);
      persistedValues.compareAndSet(id, null, new TestPayload(id));
      final var persistedValue = persistedValues.get(id);
      if (digest != null && (int) digest == persistedValue.amount) {
        loadHits.incrementAndGet(id);
        return null;
      } else {
        if (digest != null) {
          // shouldn't happen, as all items can be popped out of the cache
          fail("Cache miss: digest=" + digest + ", persistedValue.amount=" + persistedValue.amount);
        }
        loadMisses.incrementAndGet(id);
        return persistedValue.clone();
      }
    });
    when(access.sizeOf(isNotNull())).then(invocation -> (long) invocation.<TestPayload>getArgument(0).id + 1);
    when(access.digestOf(isNotNull())).then(invocation -> (int) invocation.<TestPayload>getArgument(0).amount);
    doAnswer(invocation -> {
      final var id = invocation.<Integer>getArgument(0);
      final var loadCount = loadCounts.getAndDecrement(id);
      assertEquals(1, loadCount);
      return null;
    }).when(access).unload(any(), isNotNull());

    // set up GC
    final var listener = mock(GCListener.class);
    gc = new CapacitorGC(new Options().withInterval(0).withListener(listener)).withCapacitor(capacitor);
    
    final var error = new AtomicReference<Throwable>();
    Parallel.blocking(threads, __ -> {
      for (var run = 0; run < runsPerThread; run++) {
        for (var id = 0; id < objects; id++) {
          try (var handle = capacitor.acquire(id).lock(LockMode.EXCLUSIVE)) {
            final var value = handle.value();
            value.amount++;
            final var persistedValue = persistedValues.get(id);
            persistedValue.amount++;
          } catch (Throwable e) { 
            error.set(e);
          }
        }
      }
    }).run();
    
    if (error.get() != null) {
      throw new AssertionError("An error was caught", error.get());
    }

    for (var id = 0; id < objects; id++) {
      // the amount in the persisted value should equate to the number of invocations across the contending threads
      assertEquals(threads * runsPerThread, persistedValues.get(id).amount);
      // the most recent page should line up with the persisted value
      try (var handle = capacitor.acquire(id)) {
        assertEquals(threads * runsPerThread, handle.value().amount);
      }
      assertEquals(1, loadMisses.get(id)); // one miss is expected (happens on the initial page load)
    }
    
    Timesert.wait(10_000).until(() -> {
      // verify that the collector ran at least once
      verify(listener, atLeastOnce()).reclaimCapacitor(eq(capacitor), isNotNull());
      
      // after the final collections, all pages should be unloaded
      for (var id = 0; id < objects; id++) {
        assertEquals(0, loadCounts.get(id));
      }
    });
  }
  
  /**
   *  Same test as {@link #testRaceMultipleClientsWithExpiryAndCaching()}, but uses a mixed (random) locking 
   *  mode, allowing for concurrent mutations of both the page and persisted state object.
   *  {@link TestPayloadAtomic} is used for thread safety.
   * 
   *  @throws PageLoadException 
   *  @throws NoSuchPageException 
   */
  @Test
  public void testRaceMultipleClientsWithExpiryAndCachingMixedLock() throws PageLoadException, NoSuchPageException {
    final var objects = 4;
    final var threads = 2 * Runtime.getRuntime().availableProcessors();
    final var runsPerThread = 10 * RACE_SCALE;
    
    final var persistedValues = new AtomicReferenceArray<TestPayloadAtomic>(objects);
    final var access = Classes.<PageAccess<Integer, TestPayloadAtomic>>cast(mock(PageAccess.Caching.class, Answers.CALLS_REAL_METHODS));
    final var loadCounts = new AtomicIntegerArray(objects);
    final var loadHits = new AtomicIntegerArray(objects);
    final var loadMisses = new AtomicIntegerArray(objects);
    final var capacitor = new Capacitor<>(access)
        .withResidentTtl(0)
        .withResidentCapacity(0)
        .withCache(new HeapCache<>());
    when(access.load(isNotNull(), any())).then(invocation -> {
      final var id = invocation.<Integer>getArgument(0);
      final var digest = invocation.<Integer>getArgument(1);
      final var loadCount = loadCounts.getAndIncrement(id);
      assertEquals(0, loadCount);
      persistedValues.compareAndSet(id, null, new TestPayloadAtomic(id));
      final var persistedValue = persistedValues.get(id);
      if (digest != null && (int) digest == persistedValue.amount.get()) {
        loadHits.incrementAndGet(id);
        return null;
      } else {
        if (digest != null) {
          // shouldn't happen, as all items can be popped out of the cache
          fail("Cache miss: digest=" + digest + ", persistedValue.amount=" + persistedValue.amount);
        }
        loadMisses.incrementAndGet(id);
        return persistedValue.clone();
      }
    });
    when(access.sizeOf(isNotNull())).then(invocation -> (long) invocation.<TestPayloadAtomic>getArgument(0).id + 1);
    when(access.digestOf(isNotNull())).then(invocation -> invocation.<TestPayloadAtomic>getArgument(0).amount.get());
    doAnswer(invocation -> {
      final var id = invocation.<Integer>getArgument(0);
      final var loadCount = loadCounts.getAndDecrement(id);
      assertEquals(1, loadCount);
      return null;
    }).when(access).unload(any(), isNotNull());
    
    final var error = new AtomicReference<Throwable>();
    Parallel.blocking(threads, __ -> {
      for (var run = 0; run < runsPerThread; run++) {
        for (var id = 0; id < objects; id++) {
          try (var handle = capacitor.acquire(id).lock(randomLockMode())) {
            final var value = handle.value();
            value.amount.incrementAndGet();
            final var persistedValue = persistedValues.get(id);
            persistedValue.amount.incrementAndGet();
          } catch (Throwable e) { 
            error.set(e);
          }
        }
      }
    }).run();
    
    if (error.get() != null) {
      throw new AssertionError("An error was caught", error.get());
    }

    for (var id = 0; id < objects; id++) {
      // each item should be loaded exactly once (load counts == unload counts - 1)
      assertEquals(1, loadCounts.get(id));
      // the amount in the persisted value should equate to the number of invocations across the contending threads
      assertEquals(threads * runsPerThread, persistedValues.get(id).amount.get());
      // the most recent page should line up with the persisted value
      try (var handle = capacitor.acquire(id)) {
        assertEquals(threads * runsPerThread, handle.value().amount.get());
      }
      assertEquals(1, loadMisses.get(id));
    }
  }
  
  private static LockMode randomLockMode() {
    return LockMode.values()[(int) (Math.random() * LockMode.values().length)];
  }
}
