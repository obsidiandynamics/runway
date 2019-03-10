package com.obsidiandynamics.runway.capacitor;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.concurrent.locks.*;

import org.junit.*;

import com.obsidiandynamics.assertion.*;

public final class NegotiateLockTest {
  @Test
  public void testConformance() {
    Assertions.assertUtilityClassWellDefined(NegotiateLock.class);
  }
  
  @Test
  public void testAlterNoneToExclusive() {
    final var initialMode = LockMode.NONE;
    final var lock = initialLock(initialMode);
    NegotiateLock.alter(lock, initialMode, LockMode.EXCLUSIVE);
    lock.writeLock().unlock();
    assertNotLocked(lock);
  }

  @Test
  public void testAlterSharedToExclusive() {
    final var initialMode = LockMode.SHARED;
    final var lock = initialLock(initialMode);
    NegotiateLock.alter(lock, initialMode, LockMode.EXCLUSIVE);
    lock.writeLock().unlock();
    assertNotLocked(lock);
  }

  @Test
  public void testAlterNoneToShared() {
    final var initialMode = LockMode.NONE;
    final var lock = initialLock(initialMode);
    NegotiateLock.alter(lock, initialMode, LockMode.SHARED);
    lock.readLock().unlock();
    assertNotLocked(lock);
  }

  @Test
  public void testAlterExclusiveToShared() {
    final var initialMode = LockMode.EXCLUSIVE;
    final var lock = initialLock(initialMode);
    NegotiateLock.alter(lock, initialMode, LockMode.SHARED);
    lock.readLock().unlock();
    assertNotLocked(lock);
  }

  @Test
  public void testAlterExclusiveToNone() {
    final var initialMode = LockMode.EXCLUSIVE;
    final var lock = initialLock(initialMode);
    NegotiateLock.alter(lock, initialMode, LockMode.NONE);
    assertNotLocked(lock);
  }

  @Test
  public void testAlterSharedToNone() {
    final var initialMode = LockMode.SHARED;
    final var lock = initialLock(initialMode);
    NegotiateLock.alter(lock, initialMode, LockMode.NONE);
    assertNotLocked(lock);
  }

  @Test
  public void testAlterSame() {
    final var lock = mock(ReadWriteLock.class);
    NegotiateLock.alter(lock, LockMode.NONE, LockMode.NONE);
    NegotiateLock.alter(lock, LockMode.SHARED, LockMode.SHARED);
    NegotiateLock.alter(lock, LockMode.EXCLUSIVE, LockMode.EXCLUSIVE);
    verifyNoMoreInteractions(lock);
  }
  
  private static void assertNotLocked(ReentrantReadWriteLock lock) {
    assertEquals(0, lock.getReadHoldCount());
    assertEquals(0, lock.getWriteHoldCount());
  }

  private static ReentrantReadWriteLock initialLock(LockMode lockMode) {
    final var lock = new ReentrantReadWriteLock();
    if (lockMode == LockMode.EXCLUSIVE) {
      lock.writeLock().lock();
    } else if (lockMode == LockMode.SHARED) {
      lock.readLock().lock();
    }
    return lock;
  }
}
