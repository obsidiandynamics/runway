package com.obsidiandynamics.runway.capacitor;

import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;

interface Page<I, J> {
  I getId();
  
  AtomicReference<J> getValueRef();

  ReadWriteLock getValueLock();

  void release();
}
