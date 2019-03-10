package com.obsidiandynamics.runway.capacitor;

import static com.obsidiandynamics.verifier.MethodNameFormat.*;
import static java.util.Arrays.*;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.lang.Thread.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.*;

import org.junit.*;
import org.mockito.*;

import com.obsidiandynamics.assertion.*;
import com.obsidiandynamics.await.*;
import com.obsidiandynamics.func.*;
import com.obsidiandynamics.junit.*;
import com.obsidiandynamics.mockito.*;
import com.obsidiandynamics.mockito.ResultCaptor.*;
import com.obsidiandynamics.threads.*;
import com.obsidiandynamics.verifier.*;

public final class CapacitorTest {
  static final class TestPayload implements Cloneable {
    final int id;
    int amount;
    final long created = System.currentTimeMillis();
    TestPayload(int id) { this.id = id; }
    
    TestPayload withAmount(int amount) {
      this.amount = amount;
      return this;
    }
    
    @Override
    public TestPayload clone() {
      return (TestPayload) Exceptions.wrap(super::clone, UnsupportedOperationException::new);
    }
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void testToString() {
    Assertions.assertToStringOverride(new Capacitor<>(mock(PageAccess.class)));
  }
  
  @Test
  public void testFluent() {
    FluentVerifier
    .forClass(Capacitor.class)
    .withMethodNameFormat(stripSuffix("Millis").then(stripSuffix("Units")).then(addPrefix("with")))
    .verify();
  }
  
  @Test
  public void testWithName() {
    final var access = Classes.<PageAccess<Integer, TestPayload>>cast(mock(PageAccess.NonCaching.class, Answers.CALLS_REAL_METHODS));
    final var capacitor = new Capacitor<>(access).withName("testCapacitor");
    assertEquals("testCapacitor", capacitor.getName());
  }

  @Test
  public void testAcquireRepeatedSameValue() throws PageLoadException, NoSuchPageException {
    final var access = Classes.<PageAccess<Integer, TestPayload>>cast(mock(PageAccess.NonCaching.class, Answers.CALLS_REAL_METHODS));
    final var capacitor = new Capacitor<>(access)
        .withResidentTtl(60_000);
    when(access.load(isNotNull(), any())).then(invocation -> new TestPayload(invocation.getArgument(0)));

    final TestPayload value0;
    try (var handle = capacitor.acquire(0)) {
      assertNotNull(handle);
      value0 = handle.value();
      assertEquals(0, (int) handle.id());
      assertEquals(0, value0.id);
    }

    final TestPayload value1;
    try (var handle = capacitor.acquire(0)) {
      assertNotNull(handle);
      value1 = handle.value();
      assertEquals(0, (int) handle.id());
      assertEquals(0, value1.id);
    }

    assertSame(value0, value1);

    verify(access).load(isNotNull(), isNull());
    verify(access, never()).sizeOf(isNotNull());
    verify(access, never()).digestOf(any());
    verify(access, never()).unload(any(), any());
  }

  @Test
  public void testDrain() throws PageLoadException, NoSuchPageException {
    final var access = Classes.<PageAccess<Integer, TestPayload>>cast(mock(PageAccess.NonCaching.class, Answers.CALLS_REAL_METHODS));
    final var capacitor = new Capacitor<>(access)
        .withResidentTtl(60_000);
    when(access.load(isNotNull(), any())).then(invocation -> new TestPayload(invocation.getArgument(0)));
    when(access.sizeOf(isNotNull())).thenReturn(1L);

    // load the capacitor
    try (var handle = capacitor.acquire(0)) {
      assertNotNull(handle);
    }
    assertEquals(1, capacitor.residentSize().getTotalSize());
    
    capacitor.drain();
    assertEquals(0, capacitor.residentSize().getTotalSize());

    verify(access).load(isNotNull(), isNull());
    verify(access).sizeOf(isNotNull());
    verify(access, never()).digestOf(any());
    verify(access).unload(any(), any());
    
    // retrieve handle again — should result in another load
    try (var handle = capacitor.acquire(0)) {
      assertNotNull(handle);
    }
    assertEquals(1, capacitor.residentSize().getTotalSize());
    
    verify(access, times(2)).load(isNotNull(), isNull());
    verify(access, times(2)).sizeOf(isNotNull());
    verify(access, never()).digestOf(any());
    verify(access).unload(any(), any());
  }
  
  @Test
  public void testDrainWhileInUse() throws PageLoadException, NoSuchPageException {
    final var access = Classes.<PageAccess<Integer, TestPayload>>cast(mock(PageAccess.NonCaching.class, Answers.CALLS_REAL_METHODS));
    final var capacitor = new Capacitor<>(access)
        .withResidentTtl(60_000);
    when(access.load(isNotNull(), any())).then(invocation -> new TestPayload(invocation.getArgument(0)));
    when(access.sizeOf(isNotNull())).thenReturn(1L);

    // load the capacitor and hold on to the handle
    final var handle = capacitor.acquire(0);
    assertEquals(1, capacitor.residentSize().getTotalSize());
    final var drainThread = new Thread(() -> {
      capacitor.drain();
    }, "drain thread");
    drainThread.start();
    
    // wait until the drain thread goes to sleep while waiting for the page acquisition count to drop to zero and then 
    // release the handle; this should unblock the drain
    Timesert.wait(10_000).untilTrue(threadInState(drainThread, State.TIMED_WAITING));
    handle.close();
  }

  @Test
  public void testAcquireAndReplace() throws PageLoadException, NoSuchPageException {
    final var access = Classes.<PageAccess<Integer, TestPayload>>cast(mock(PageAccess.NonCaching.class, Answers.CALLS_REAL_METHODS));
    final var capacitor = new Capacitor<>(access)
        .withResidentTtl(60_000);
    when(access.load(isNotNull(), any())).then(invocation -> new TestPayload(invocation.getArgument(0)));

    final TestPayload value0;
    try (var handle = capacitor.acquire(0).lock(LockMode.EXCLUSIVE)) {
      assertNotNull(handle);
      value0 = handle.value();
      assertEquals(0, (int) handle.id());
      assertEquals(0, value0.id);
      
      value0.amount = 100;
      assertEquals(100, handle.value().amount);
      
      handle.replace(new TestPayload(0).withAmount(200));
      assertEquals(200, handle.value().amount);
    }

    final TestPayload value1;
    try (var handle = capacitor.acquire(0)) {
      assertNotNull(handle);
      value1 = handle.value();
      assertEquals(0, (int) handle.id());
      assertEquals(200, value1.amount);
    }

    assertNotSame(value0, value1);

    verify(access).load(isNotNull(), isNull());
    verify(access, never()).sizeOf(isNotNull());
    verify(access, never()).digestOf(any());
    verify(access, never()).unload(any(), any());
  }

  @Test
  public void testAcquireAndReload() throws PageLoadException, NoSuchPageException {
    final var access = Classes.<PageAccess<Integer, TestPayload>>cast(mock(PageAccess.NonCaching.class, Answers.CALLS_REAL_METHODS));
    final var capacitor = new Capacitor<>(access)
        .withResidentTtl(60_000);
    when(access.load(isNotNull(), any())).then(invocation -> new TestPayload(invocation.getArgument(0)));

    final TestPayload value0;
    try (var handle = capacitor.acquire(0).lock(LockMode.EXCLUSIVE)) {
      assertNotNull(handle);
      value0 = handle.value();
      assertEquals(0, (int) handle.id());
      assertEquals(0, value0.id);
      
      value0.amount = 100;
      assertEquals(100, handle.value().amount);
      handle.reload();
    }

    final TestPayload value1;
    try (var handle = capacitor.acquire(0)) {
      assertNotNull(handle);
      value1 = handle.value();
      assertEquals(0, (int) handle.id());
      assertEquals(0, value1.amount);
    }

    assertNotSame(value0, value1);

    verify(access, times(2)).load(isNotNull(), isNull());
    verify(access, never()).sizeOf(isNotNull());
    verify(access, never()).digestOf(any());
    verify(access, never()).unload(any(), any());
  }

  @Test(expected=PageLoadException.class)
  public void acquireFaultOnLoad() throws PageLoadException, NoSuchPageException {
    final var accessType = Classes.<Class<PageAccess<Integer, TestPayload>>>cast(PageAccess.NonCaching.class);
    final var access = mock(accessType, Answers.CALLS_REAL_METHODS);
    final var capacitor = new Capacitor<>(access)
        .withResidentTtl(60_000);
    when(access.load(isNotNull(), any())).then(invocation -> {
      throw new PageLoadException("Simulated faulty load", null);
    });

    try (var handle = capacitor.acquire(0)) {}
  }

  @Test
  public void acquireOneOffFaultOnLoadWithSubsequentSuccess() throws PageLoadException, NoSuchPageException {
    final var access = Classes.<PageAccess<Integer, TestPayload>>cast(mock(PageAccess.NonCaching.class, Answers.CALLS_REAL_METHODS));
    final var loadAttempts = new AtomicInteger();
    final var capacitor = new Capacitor<>(access)
        .withResidentTtl(60_000);
    final var latch = new CountDownLatch(1);
    final var error = new AtomicReference<Throwable>();
    when(access.load(isNotNull(), any())).then(invocation -> {
      final var id = invocation.<Integer>getArgument(0);
      if (loadAttempts.getAndIncrement() == 0) {
        final var secondAcquisitionThread = new Thread(() -> {
          // Attempt to do a second acquisition here, which should be locked internally until the first fails.
          // When we eventually acquire the page, it will be the same page instance, and should be marked as disposed. This will
          // result in another load attempt, which should succeed.
          try {
            capacitor.acquire(id);
          } catch (Throwable e) {
            e.printStackTrace();
            error.set(e);
          } finally {
            latch.countDown();
          }
        }, "Second acquisition");
        secondAcquisitionThread.start();
        // give the thread a chance to start give the thread a chance to start ...
        Timesert.wait(1000).untilTrue(threadInState(secondAcquisitionThread, State.BLOCKED));
        throw new PageLoadException("Simulated faulty load", null);
      } else {
        return new TestPayload(id);
      }
    });

    // the first acquisition should fail with an exception
    try (var handle = capacitor.acquire(0)) {
      fail("Expected " + PageLoadException.class);
    } catch (PageLoadException e) {
      // all good, failure is expected for the first call to acquire()
    } catch (Throwable e) {
      fail("Expected " + PageLoadException.class + ", got " + Throwable.class);
    }

    // wait for the second acquisition to complete
    Threads.await(latch);
    assertNull("error=" + error, error.get());

    verify(access, times(2)).load(isNotNull(), isNull());
    verify(access, never()).sizeOf(isNotNull());
    verify(access, never()).digestOf(any());
    verify(access, never()).unload(any(), any());
  }

  @Test
  public void testLoadWithSubsequentImplicitExpiry() throws PageLoadException, NoSuchPageException {
    final var access = Classes.<PageAccess<Integer, TestPayload>>cast(mock(PageAccess.NonCaching.class, Answers.CALLS_REAL_METHODS));
    final var capacitor = new Capacitor<>(access)
        .withResidentTtl(0);
    when(access.load(isNotNull(), any())).then(invocation -> new TestPayload(invocation.getArgument(0)));

    final TestPayload value0;
    try (var handle = capacitor.acquire(0)) {
      assertNotNull(handle);
      value0 = handle.value();
      assertEquals(0, (int) handle.id());
      assertEquals(0, value0.id);
    }

    Threads.sleep(10); // give it time to expire... next acquire() will result in a new load

    final TestPayload value1;
    try (var handle = capacitor.acquire(0)) {
      assertNotNull(handle);
      value1 = handle.value();
      assertEquals(0, (int) handle.id());
      assertEquals(0, value1.id);
    }

    assertNotSame(value0, value1);
    assertNotEquals(value0.created, value1.created);

    verify(access, times(2)).load(isNotNull(), isNull());
    verify(access).sizeOf(isNotNull());
    verify(access, never()).digestOf(any());
    verify(access).unload(isNotNull(), any());
  }

  /**
   *  Tests a scenario where the capacitor holds on to an otherwise expired page because at least
   *  one consumers is has an open handle to it.
   *  
   *  @throws PageLoadException
   *  @throws NoSuchPageException 
   */
  @Test
  public void testLoadWithNoExpiry() throws PageLoadException, NoSuchPageException {
    final var access = Classes.<PageAccess<Integer, TestPayload>>cast(mock(PageAccess.NonCaching.class, Answers.CALLS_REAL_METHODS));
    final var capacitor = new Capacitor<>(access)
        .withResidentTtl(0);
    when(access.load(isNotNull(), any())).then(invocation -> new TestPayload(invocation.getArgument(0)));

    final TestPayload value0;
    final var handle0 = capacitor.acquire(0);
    assertNotNull(handle0);
    value0 = handle0.value();
    assertEquals(0, (int) handle0.id());
    assertEquals(0, value0.id);

    Threads.sleep(10); // give it time to expire... but don't release the handle

    final TestPayload value1;
    try (var handle1 = capacitor.acquire(0)) {
      assertNotNull(handle1);
      value1 = handle1.value();
      assertEquals(0, (int) handle1.id());
      assertEquals(0, value1.id);
    }

    assertSame(value0, value1);
    verify(access).load(isNotNull(), isNull());
    verify(access, never()).sizeOf(isNotNull());
    verify(access, never()).digestOf(any());
    verify(access, never()).unload(any(), any());
  }

  @Test
  public void testSizeOfAndGetResidentSize() throws PageLoadException, NoSuchPageException {
    final var access = Classes.<PageAccess<Integer, TestPayload>>cast(mock(PageAccess.NonCaching.class, Answers.CALLS_REAL_METHODS));
    final var capacitor = new Capacitor<>(access)
        .withResidentTtl(0);
    when(access.load(isNotNull(), any())).then(invocation -> new TestPayload(invocation.getArgument(0)));
    when(access.sizeOf(isNotNull())).then(invocation -> 10L * (invocation.<TestPayload>getArgument(0).id + 1));

    assertEquals(10, capacitor.sizeOf(0));
    assertEquals(20, capacitor.sizeOf(1));

    final var stats = capacitor.residentSize();
    assertEquals(30, stats.getTotalSize());
    assertEquals(MapBuilder.init(0, 10L).with(1, 20L).build(), stats.getIndividualStats());

    verify(access, times(2)).load(isNotNull(), isNull());
    verify(access, times(4)).sizeOf(isNotNull());
    verify(access, never()).digestOf(any());
    verify(access, never()).unload(any(), any());
  }

  @Test
  public void testResidentSizeWhileLoading() throws PageLoadException, NoSuchPageException {
    final var access = Classes.<PageAccess<Integer, TestPayload>>cast(mock(PageAccess.NonCaching.class, Answers.CALLS_REAL_METHODS));
    final var capacitor = new Capacitor<>(access)
        .withResidentTtl(0);
    when(access.load(isNotNull(), any())).then(invocation -> {
      // While the page is being loaded, run the getResidentSize() query, which should acquire the page lock (due to
      // it being reentrant) and come across the page in the middle of the loading process. Because the page will at
      // that point still be empty, it will not be included in the total size.
      final var stats = capacitor.residentSize();
      assertEquals(0, stats.getTotalSize());
      assertEquals(Collections.emptyMap(), stats.getIndividualStats());
      return new TestPayload(invocation.getArgument(0));
    });
    when(access.sizeOf(isNotNull())).then(invocation -> 10L * (invocation.<TestPayload>getArgument(0).id + 1));

    assertEquals(10, capacitor.sizeOf(0));

    verify(access).load(isNotNull(), isNull());
    verify(access).sizeOf(isNotNull());
    verify(access, never()).digestOf(any());
    verify(access, never()).unload(any(), any());
  }

  /**
   *  Objects are not reclaimed if they are still within the TTL, providing the capacity limits
   *  are not breached.
   *  
   *  @throws PageLoadException
   *  @throws NoSuchPageException 
   */
  @Test
  public void testNoReclaimWithinTtlWithinCapacityLimit() throws PageLoadException, NoSuchPageException {
    final var access = Classes.<PageAccess<Integer, TestPayload>>cast(mock(PageAccess.NonCaching.class, Answers.CALLS_REAL_METHODS));
    final var capacitor = new Capacitor<>(access)
        .withResidentTtl(60_000);
    when(access.load(isNotNull(), any())).then(invocation -> new TestPayload(invocation.getArgument(0)));
    when(access.sizeOf(isNotNull())).then(invocation -> 10L * (invocation.<TestPayload>getArgument(0).id + 1));

    capacitor.acquire(0).close();

    final var reclaim = capacitor.reclaimResidents();
    assertEquals(0, reclaim.getReclaimedSpace());
    assertEquals(Collections.emptyList(), reclaim.getPurgedIds());

    verify(access).load(isNotNull(), isNull());
    verify(access).sizeOf(isNotNull());
    verify(access, never()).digestOf(any());
    verify(access, never()).unload(any(), any());
  }

  /**
   *  Reclaims expired residents within the capacity limit. Objects will be reclaimed if they have
   *  expired.
   *  
   *  @throws PageLoadException
   *  @throws NoSuchPageException 
   */
  @Test
  public void testReclaimResidentsWithinCapacity() throws PageLoadException, NoSuchPageException {
    final var access = Classes.<PageAccess<Integer, TestPayload>>cast(mock(PageAccess.NonCaching.class, Answers.CALLS_REAL_METHODS));
    final var capacitor = new Capacitor<>(access)
        .withResidentTtl(0);
    when(access.load(isNotNull(), any())).then(invocation -> new TestPayload(invocation.getArgument(0)));
    when(access.sizeOf(isNotNull())).then(invocation -> 10L * (invocation.<TestPayload>getArgument(0).id + 1));

    final var initialReclaim = capacitor.reclaimResidents();
    assertEquals(0, initialReclaim.getReclaimedSpace());
    assertEquals(Collections.emptyList(), initialReclaim.getPurgedIds());

    capacitor.acquire(0).close();
    capacitor.acquire(1).close();
    assertEquals(30, capacitor.residentSize().getTotalSize());

    Threads.sleep(10); // give it some time to expire the objects

    final var reclaim = capacitor.reclaimResidents();
    assertEquals(30, reclaim.getReclaimedSpace());
    assertEquals(new HashSet<>(asList(0, 1)), new HashSet<>(reclaim.getPurgedIds()));

    verify(access, times(2)).load(isNotNull(), isNull());
    verify(access, times(4)).sizeOf(isNotNull());
    verify(access, never()).digestOf(any());
    verify(access, times(2)).unload(isNotNull(), isNotNull());
  }

  /**
   *  Reclaiming objects while they are in use. An object cannot be reclaimed if it is used
   *  by at least one consumer.
   *  
   *  @throws PageLoadException
   *  @throws NoSuchPageException 
   */
  @Test
  public void testNoReclaimWhilePageInUse() throws PageLoadException, NoSuchPageException {
    final var access = Classes.<PageAccess<Integer, TestPayload>>cast(mock(PageAccess.NonCaching.class, Answers.CALLS_REAL_METHODS));
    final var capacitor = new Capacitor<>(access)
        .withResidentTtl(0);
    when(access.load(isNotNull(), any())).then(invocation -> new TestPayload(invocation.getArgument(0)));
    when(access.sizeOf(isNotNull())).then(invocation -> 10L * (invocation.<TestPayload>getArgument(0).id + 1));

    // acquire one of the objects and don't let go until the test completes
    final var handle = capacitor.acquire(0);

    capacitor.acquire(1).close();
    assertEquals(30, capacitor.residentSize().getTotalSize());

    Threads.sleep(10); // give it some time to expire the objects

    final var reclaim = capacitor.reclaimResidents();
    assertEquals(20, reclaim.getReclaimedSpace());
    assertEquals(new HashSet<>(asList(1)), new HashSet<>(reclaim.getPurgedIds()));

    handle.close();

    verify(access, times(2)).load(isNotNull(), isNull());
    verify(access, times(4)).sizeOf(isNotNull());
    verify(access, never()).digestOf(any());
    verify(access).unload(eq(1), isNotNull()); // only one object will get unloaded
    verify(access, never()).unload(eq(0), any());
  }

  /**
   *  Reclaiming of objects while they are being loaded. A page can only be reclaimed if it is in
   *  the {@link PageStatus#READY} state; otherwise the algorithm should ignore it.
   *  
   *  @throws PageLoadException
   *  @throws NoSuchPageException 
   */
  @Test
  public void testNoReclaimWhileLoading() throws PageLoadException, NoSuchPageException {
    final var access = Classes.<PageAccess<Integer, TestPayload>>cast(mock(PageAccess.NonCaching.class, Answers.CALLS_REAL_METHODS));
    final var capacitor = new Capacitor<>(access)
        .withResidentTtl(0);
    when(access.load(isNotNull(), any())).then(invocation -> {
      Thread.sleep(10); // give it time to expire
      // While the page is loading, run reclaimResidents(). This gets access to the page lock (due to reentrancy) and
      // will avoid reclaiming the page as it hasn't yet been loaded.
      final var reclaim = capacitor.reclaimResidents();
      assertEquals(0, reclaim.getReclaimedSpace());
      assertEquals(Collections.emptyList(), reclaim.getPurgedIds());
      return new TestPayload(invocation.getArgument(0));
    });
    when(access.sizeOf(isNotNull())).then(invocation -> 10L * (invocation.<TestPayload>getArgument(0).id + 1));

    capacitor.acquire(0).close();

    verify(access).load(isNotNull(), isNull());
    verify(access, never()).sizeOf(isNotNull());
    verify(access, never()).digestOf(any());
    verify(access, never()).unload(any(), any());
  }

  /**
   *  Tests the scenario where an object is reclaimed, but its size was not available in the {@link SizeStats}
   *  object — the object was added after the initial sizes were established, which warrants an on-demand calculation
   *  during disposal.
   *  
   *  @throws PageLoadException
   *  @throws NoSuchPageException 
   */
  @Test
  public void testReclaimWithInlineSizeCalculation() throws PageLoadException, NoSuchPageException {
    final var access = Classes.<PageAccess<Integer, TestPayload>>cast(mock(PageAccess.NonCaching.class, Answers.CALLS_REAL_METHODS));
    final var capacitor = new Capacitor<>(access)
        .withResidentTtl(0);
    when(access.load(isNotNull(), any())).then(invocation -> new TestPayload(invocation.getArgument(0)));
    when(access.sizeOf(isNotNull())).then(invocation -> {
      final var id = invocation.<TestPayload>getArgument(0).id;
      if (id == 0) {
        // Before returning the size, add another object to the capacitor. When that object gets reclaimed, the
        // sizes would already have been calculated, sans the newly added object.
        capacitor.acquire(1).close();
        Thread.sleep(10); // give it time to expire
      }
      return 10L * (id + 1);
    });

    capacitor.acquire(0).close();
    final var reclaim = capacitor.reclaimResidents();
    assertEquals(30, reclaim.getReclaimedSpace());
    assertEquals(new HashSet<>(asList(0, 1)), new HashSet<>(reclaim.getPurgedIds()));

    verify(access, times(2)).load(isNotNull(), isNull());
    verify(access, times(2)).sizeOf(isNotNull());
    verify(access, never()).digestOf(any());
    verify(access, times(2)).unload(isNotNull(), isNotNull());
  }

  /**
   *  Reclaiming objects that are within their TTLs, but the capacitor is running over the
   *  maximum allowable capacity. Pages should be reclaimed, providing they are not in use.
   *  
   *  @throws PageLoadException
   *  @throws NoSuchPageException 
   */
  @Test
  public void testReclaimWithinTtlOverCapacity() throws PageLoadException, NoSuchPageException {
    final var access = Classes.<PageAccess<Integer, TestPayload>>cast(mock(PageAccess.NonCaching.class, Answers.CALLS_REAL_METHODS));
    final var capacitor = new Capacitor<>(access)
        .withResidentTtl(60_000)
        .withResidentCapacity(0);
    when(access.load(isNotNull(), any())).then(invocation -> new TestPayload(invocation.getArgument(0)));
    when(access.sizeOf(isNotNull())).then(invocation -> 10L * (invocation.<TestPayload>getArgument(0).id + 1));

    capacitor.acquire(0).close();
    capacitor.acquire(1).close();

    final var reclaim = capacitor.reclaimResidents();
    assertEquals(30, reclaim.getReclaimedSpace());
    assertEquals(new HashSet<>(asList(0, 1)), new HashSet<>(reclaim.getPurgedIds()));

    verify(access, times(2)).load(isNotNull(), isNull());
    verify(access, times(2)).sizeOf(isNotNull());
    verify(access, never()).digestOf(any());
    verify(access, times(2)).unload(isNotNull(), isNotNull());
  }

  /**
   *  Tests the scenario where we're over capacity and are reclaiming pages that are within their TTL, but as
   *  the pages are being reclaimed, the size of resident set drops to within the allowable capacity and only
   *  some of the candidate objects need to be removed.
   *  
   *  @throws PageLoadException
   *  @throws NoSuchPageException 
   */
  @Test
  public void testReclaimWithinTtlDownToJustUnderCapacity() throws PageLoadException, NoSuchPageException {
    final var access = Classes.<PageAccess<Integer, TestPayload>>cast(mock(PageAccess.NonCaching.class, Answers.CALLS_REAL_METHODS));
    final var capacitor = new Capacitor<>(access)
        .withResidentTtl(60_000)
        .withResidentCapacity(20);
    when(access.load(isNotNull(), any())).then(invocation -> new TestPayload(invocation.getArgument(0)));
    when(access.sizeOf(isNotNull())).then(invocation -> 10L);

    capacitor.acquire(0).close();
    capacitor.acquire(1).close();
    capacitor.acquire(2).close();

    final var reclaim = capacitor.reclaimResidents();
    assertEquals(10, reclaim.getReclaimedSpace());
    assertEquals(1, reclaim.getPurgedIds().size());
    assertThat(reclaim.getPurgedIds(), HamcrestMatchers.fulfils(purgedIds -> {
      return purgedIds.contains(0) || purgedIds.contains(1) || purgedIds.contains(2);
    }));

    verify(access, times(3)).load(isNotNull(), isNull());
    verify(access, times(3)).sizeOf(isNotNull());
    verify(access, never()).digestOf(any());
    verify(access, times(1)).unload(isNotNull(), isNotNull()); // just one object needs to be reclaimed
  }

  /**
   *  Reclaims resident pages and verifies that the objects end up in the cache.
   *  
   *  @throws PageLoadException
   *  @throws NoSuchPageException 
   */
  @Test
  public void testReclaimToCache() throws PageLoadException, NoSuchPageException {
    final var access = Classes.<PageAccess<Integer, TestPayload>>cast(mock(PageAccess.Caching.class, Answers.CALLS_REAL_METHODS));
    final var capacitor = new Capacitor<>(access)
        .withResidentTtl(0)
        .withResidentCapacity(0)
        .withCache(new HeapCache<>());
    when(access.load(isNotNull(), any())).then(invocation -> new TestPayload(invocation.getArgument(0)));
    when(access.sizeOf(isNotNull())).then(invocation -> 10L * (invocation.<TestPayload>getArgument(0).id + 1));
    
    capacitor.acquire(0).close();
    capacitor.acquire(1).close();
    
    final var reclaim = capacitor.reclaimResidents();
    assertEquals(30, reclaim.getReclaimedSpace());
    assertEquals(2, reclaim.getPurgedIds().size());
    
    final var cacheSize = capacitor.getCache().localSize();
    assertEquals(30, cacheSize.getTotalSize());
    assertEquals(MapBuilder.init(0, 10L).with(1, 20L).build(), cacheSize.getIndividualStats());
    
    verify(access, times(2)).load(isNotNull(), isNull());
    verify(access, times(2)).sizeOf(isNotNull());
    verify(access, never()).digestOf(any());
    verify(access, times(2)).unload(isNotNull(), isNotNull());
  }
  
  /**
   *  Loads the value from the cache with the digest matching; the load operation should 
   *  return {@code null}, honouring the cached value.
   *  
   *  @throws PageLoadException
   *  @throws NoSuchPageException 
   */
  @Test
  public void testLoadFromCacheWithMatchingDigest() throws PageLoadException, NoSuchPageException {
    final var access = Classes.<PageAccess<Integer, TestPayload>>cast(mock(PageAccess.Caching.class, Answers.CALLS_REAL_METHODS));
    final var capacitor = new Capacitor<>(access)
        .withResidentTtl(60_000)
        .withResidentCapacity(0)
        .withCache(new HeapCache<>());
    final ResultCaptor<?> loadResult;
    when(access.load(isNotNull(), any())).then(loadResult = ResultCaptor.of(invocation -> {
      final var id = invocation.<Integer>getArgument(0);
      final var digest = invocation.<Integer>getArgument(1);
      if (digest != null && (int) digest == id) {
        return null; // <- this is the expected path
      } else {
        return new TestPayload(id);
      }
    }));
    when(access.sizeOf(isNotNull())).then(invocation -> 10L * (invocation.<TestPayload>getArgument(0).id + 1));
    when(access.digestOf(isNotNull())).then(invocation -> invocation.<TestPayload>getArgument(0).id);
    
    capacitor.getCache().put(0, new TestPayload(0), 10);
    capacitor.getCache().put(1, new TestPayload(1), 20);

    assertEquals(SizeStats.empty(), capacitor.residentSize());
    assertEquals(30, capacitor.getCache().localSize().getTotalSize());
    
    capacitor.acquire(0).close();
    capacitor.acquire(1).close();
    
    assertEquals(Capture.empty(), loadResult.get(0, 0));
    assertEquals(Capture.empty(), loadResult.get(1, 1));

    // fetching the page from the cache has the effect of removing it
    assertEquals(SizeStats.empty(), capacitor.getCache().localSize());
    
    verify(access, times(2)).load(isNotNull(), isNotNull());
    verify(access, never()).load(isNotNull(), isNull());
    verify(access, never()).sizeOf(any());
    verify(access, times(2)).digestOf(isNotNull());
    verify(access, never()).unload(any(), any());
  }
  
  /**
   *  Loads the value from the cache with a non-matching digest; the load operation should 
   *  return the persisted object, ignoring the cached value.
   *  
   *  @throws PageLoadException
   *  @throws NoSuchPageException 
   */
  @Test
  public void testLoadFromCacheWithNonMatchingDigest() throws PageLoadException, NoSuchPageException {
    final var access = Classes.<PageAccess<Integer, TestPayload>>cast(mock(PageAccess.Caching.class, Answers.CALLS_REAL_METHODS));
    final var capacitor = new Capacitor<>(access)
        .withResidentTtl(60_000)
        .withResidentCapacity(0)
        .withCache(new HeapCache<>());
    final ResultCaptor<?> loadResult;
    when(access.load(isNotNull(), any())).then(loadResult = ResultCaptor.of(invocation -> {
      final var id = invocation.<Integer>getArgument(0);
      final var digest = invocation.<Integer>getArgument(1);
      if (digest != null && (int) digest == id) {
        return null;
      } else {
        return new TestPayload(id);  // <- this is the expected path
      }
    }));
    when(access.sizeOf(isNotNull())).then(invocation -> 10L * (invocation.<TestPayload>getArgument(0).id + 1));
    when(access.digestOf(isNotNull())).then(invocation -> invocation.<TestPayload>getArgument(0).id + 100);
    
    capacitor.getCache().put(0, new TestPayload(0), 10);
    capacitor.getCache().put(1, new TestPayload(1), 20);

    assertEquals(SizeStats.empty(), capacitor.residentSize());
    assertEquals(30, capacitor.getCache().localSize().getTotalSize());
    
    capacitor.acquire(0).close();
    capacitor.acquire(1).close();
    
    assertNotNull(loadResult.get(0, 100).result());
    assertNotNull(loadResult.get(1, 101).result());

    // fetching the page from the cache has the effect of removing it, even though the cached entries got rejected
    assertEquals(SizeStats.empty(), capacitor.getCache().localSize());
    
    verify(access, times(2)).load(isNotNull(), isNotNull());
    verify(access, never()).load(isNotNull(), isNull());
    verify(access, never()).sizeOf(any());
    verify(access, times(2)).digestOf(isNotNull());
    verify(access, never()).unload(any(), any());
  }

  private static BooleanSupplier threadInState(Thread thread, State state) {
    return () -> thread.getState() == state;
  }
}
