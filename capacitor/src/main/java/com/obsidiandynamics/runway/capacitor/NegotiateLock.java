package com.obsidiandynamics.runway.capacitor;

import java.util.concurrent.locks.*;

final class NegotiateLock {
  private NegotiateLock() {}
  
  static void alter(ReadWriteLock lock, LockMode from, LockMode to) {
    if (from == to) return;
    
    if (to == LockMode.EXCLUSIVE) {
      if (from == LockMode.SHARED) {
        lock.readLock().unlock();
      }
      lock.writeLock().lock();
    } else if (to == LockMode.SHARED) {
      lock.readLock().lock();
      if (from == LockMode.EXCLUSIVE) {
        lock.writeLock().unlock();
      }
    } else {
      if (from == LockMode.EXCLUSIVE) {
        lock.writeLock().unlock();
      } else {
        lock.readLock().unlock();
      }
    }
  }
}
