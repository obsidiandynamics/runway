package com.obsidiandynamics.runway.capacitor;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;

import org.junit.*;
import org.mockito.*;

import com.obsidiandynamics.assertion.*;
import com.obsidiandynamics.func.*;

public final class PageHandleTest {
  @Test
  public void testAccessorsAndToString() {
    final var page = Mockito.<Page<Integer, String>>mock(Classes.cast(Page.class));
    when(page.getId()).thenReturn(100);
    when(page.getValueRef()).thenReturn(new AtomicReference<>("string"));
    
    try (var handle = new PageHandle<>(null, page)) {
      assertEquals(100, (int) handle.id());
      assertEquals("string", handle.value());
      assertEquals(LockMode.NONE, handle.getLockMode());
      Assertions.assertToStringOverride(handle);
    }
  }
  
  @Test
  public void testLock() {
    final var page = Mockito.<Page<Integer, String>>mock(Classes.cast(Page.class));
    final var lock = new ReentrantReadWriteLock();
    when(page.getValueLock()).thenReturn(lock);
    
    try (var handle = new PageHandle<>(null, page)) {
      handle.lock(LockMode.SHARED);
      assertEquals(1, lock.getReadHoldCount());
      assertEquals(0, lock.getWriteHoldCount());

      handle.lock(LockMode.EXCLUSIVE);
      assertEquals(0, lock.getReadHoldCount());
      assertEquals(1, lock.getWriteHoldCount());
      
      handle.lock(LockMode.NONE);
      assertEquals(0, lock.getReadHoldCount());
      assertEquals(0, lock.getWriteHoldCount());
    }
  }
  
  @Test
  public void testRelease() {
    final var page = Mockito.<Page<Integer, String>>mock(Classes.cast(Page.class));
    final var lock = new ReentrantReadWriteLock();
    when(page.getValueLock()).thenReturn(lock);
    
    try (var handle = new PageHandle<>(null, page)) {
      handle.lock(LockMode.SHARED);
      assertEquals(1, lock.getReadHoldCount());
      assertEquals(0, lock.getWriteHoldCount());
      
      handle.close();
      assertEquals(0, lock.getReadHoldCount());
      assertEquals(0, lock.getWriteHoldCount());
      verify(page).release();
    }
    
    // second release (as party of the try-with-resource) should have no further effect
    verify(page).release();
  }
  
  @Test
  public void testReplaceSuccess() {
    final var page = Mockito.<Page<Integer, String>>mock(Classes.cast(Page.class));
    final var lock = new ReentrantReadWriteLock();
    final var valueRef = new AtomicReference<String>("value");
    when(page.getValueLock()).thenReturn(lock);
    when(page.getValueRef()).thenReturn(valueRef);
    
    try (var handle = new PageHandle<>(null, page)) {
      handle.lock(LockMode.EXCLUSIVE);
      assertEquals("value", handle.value());
      
      handle.replace("newValue");
      assertEquals("newValue", handle.value());
    }
    
    // second acquisition should produce the new value
    try (var handle = new PageHandle<>(null, page)) {
      assertEquals("newValue", handle.value());
    }
  }
  
  @Test(expected=IllegalStateException.class)
  public void testReplaceWrongLock() {
    final var page = Mockito.<Page<Integer, String>>mock(Classes.cast(Page.class));
    final var lock = new ReentrantReadWriteLock();
    final var valueRef = new AtomicReference<String>("value");
    when(page.getValueLock()).thenReturn(lock);
    when(page.getValueRef()).thenReturn(valueRef);
    
    try (var handle = new PageHandle<>(null, page)) {
      handle.lock(LockMode.SHARED);
      handle.replace("newValue");
    }
  }
  
  @Test
  public void testUseAfterClose() {
    final var page = Mockito.<Page<Integer, String>>mock(Classes.cast(Page.class));
    final var lock = new ReentrantReadWriteLock();
    when(page.getValueLock()).thenReturn(lock);
    final var handle = new PageHandle<>(null, page);
    handle.close();
    
    org.assertj.core.api.Assertions.assertThatThrownBy(() -> handle.lock(LockMode.SHARED))
    .isInstanceOf(IllegalStateException.class);
    
    org.assertj.core.api.Assertions.assertThatThrownBy(() -> handle.value())
    .isInstanceOf(IllegalStateException.class);
  }
}
