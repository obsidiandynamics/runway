package com.obsidiandynamics.runway.capacitor;

import static com.obsidiandynamics.verifier.MethodNameFormat.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.util.*;

import org.junit.*;
import org.mockito.*;

import com.obsidiandynamics.runway.capacitor.CapacitorGC.*;
import com.obsidiandynamics.await.*;
import com.obsidiandynamics.func.*;
import com.obsidiandynamics.threads.*;
import com.obsidiandynamics.verifier.*;
import com.obsidiandynamics.zerolog.*;

public final class CapacitorGCTest {
  private CapacitorGC gc;
  
  @After
  public void after() {
    if (gc != null) {
      gc.terminate().joinSilently();
      gc = null;
    }
  }
  
  @Test
  public void testOptionsFluent() {
    FluentVerifier
    .forClass(Options.class)
    .withMethodNameFormat(stripSuffix("Millis").then(addPrefix("with")))
    .verify();
  }
  
  @Test
  public void testZlgListenerFluent() {
    FluentVerifier
    .forClass(ZlgGCListener.class)
    .verify();
  }
  
  @Test
  public void testListener() {
    final var access = Classes.<PageAccess<Integer, String>>cast(mock(PageAccess.NonCaching.class, Answers.CALLS_REAL_METHODS));
    final var capacitor = new Capacitor<>(access).withName("testCapacitor");
    
    final var logTarget = new MockLogTarget();
    final var listener = new ZlgGCListener().withZlg(logTarget.logger()).withLevel(LogLevel.INFO);
    
    listener.reclaimCapacitor(capacitor, ReclaimStats.empty());
    logTarget.entries().forLevel(LogLevel.INFO).containing(capacitor.getName()).assertCount(1);
    logTarget.entries().assertCount(1);
    logTarget.reset();
    
    listener.reclaimCache(capacitor, 0, ReclaimStats.empty());
    logTarget.entries().forLevel(LogLevel.INFO).containing(capacitor.getName()).assertCount(1);
    logTarget.entries().assertCount(1);
    logTarget.reset();
  }
  
  @Test
  public void testGCCapacitorAndCachesWithPurgedItems() throws PageLoadException, NoSuchPageException {
    final var access = Classes.<PageAccess<Integer, String>>cast(mock(PageAccess.Caching.class, Answers.CALLS_REAL_METHODS));
    when(access.load(isNotNull(), any())).thenReturn("value");
    when(access.sizeOf(isNotNull())).thenReturn(1L);
    when(access.digestOf(isNotNull())).thenReturn(1);
    
    final var capacitor = new Capacitor<>(access).withName("testCapacitor")
        .withResidentTtl(0)
        .withCache(new HeapCache<Integer, String>()
                   .withLocalTtl(0)
                   .withNextTier(new HeapCache<Integer, String>()
                                 .withLocalTtl(0)));
    
    // load the capacitor with a single value, and let it expire
    try (var handle = capacitor.acquire(0)) {}
    verify(access).load(eq(0), isNull());
    Threads.sleep(1);
    
    final var listener = mock(GCListener.class);
    doAnswer(invocation -> {
      // by pausing in the listener, we give enough time for the item that was pushed to the cache to expire
      Threads.sleep(1);
      return null;
    }).when(listener).reclaimCapacitor(isNotNull(), isNotNull());
    doAnswer(invocation -> {
      // by pausing in the listener, we give enough time for the item that was pushed to the next tier to expire
      Threads.sleep(1);
      return null;
    }).when(listener).reclaimCache(isNotNull(), anyInt(), isNotNull());
    
    gc = new CapacitorGC(new Options().withInterval(0).withListener(listener)).withCapacitor(capacitor);
    final var singleUnitReclaim = new ReclaimStats<>(1, Collections.singletonList(0));
    Timesert.wait(10_000).until(() -> {
      verify(listener).reclaimCapacitor(eq(capacitor), eq(singleUnitReclaim));
      verify(listener).reclaimCache(eq(capacitor), eq(0), eq(singleUnitReclaim));
      verify(listener).reclaimCache(eq(capacitor), eq(1), eq(singleUnitReclaim));
    });
  }
  
  @Test
  public void testGCCapacitorWithNoPurgedItems() throws PageLoadException, NoSuchPageException {
    final var access = Classes.<PageAccess<Integer, String>>cast(mock(PageAccess.NonCaching.class, Answers.CALLS_REAL_METHODS));
    when(access.load(isNotNull(), any())).thenReturn("value");
    when(access.sizeOf(isNotNull())).thenReturn(1L);
    
    final var capacitor = new Capacitor<>(access).withName("testCapacitor");
    final var listener = mock(GCListener.class);
    gc = new CapacitorGC(new Options().withInterval(0).withListener(listener)).withCapacitor(capacitor);
    
    // allow some time for the capacitor's reclaimResidents() method to be called
    Threads.sleep(50);
    
    // verify that the listener was never called
    verifyNoMoreInteractions(listener);
  }
}
