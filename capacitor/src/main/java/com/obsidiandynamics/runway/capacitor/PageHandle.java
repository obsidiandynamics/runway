package com.obsidiandynamics.runway.capacitor;

import static com.obsidiandynamics.func.Functions.*;

/**
 *  A single-use façade for accessing objects retained in a {@link Capacitor}. <p>
 *  
 *  Once a handle is obtained through {@link Capacitor#acquire(Object)}, a lock must be acquired
 *  before the encapsulated object can be accessed. Depending on whether the object will be read
 *  from or written to, a {@link LockMode#SHARED} or a {@link LockMode#EXCLUSIVE} lock mode
 *  must first be negotiated with a call to {@link #lock(LockMode)}. The negotiation
 *  of a lock mode is a blocking action. Once negotiated, the object may be retrieved by calling
 *  {@link PageHandle#value()}. <p>
 *  
 *  A {@link PageHandle} instance is short-lived, and valid only for the duration of the
 *  acquisition scope. Closing a handle disposes it and indicates to the capacitor that the
 *  in-memory page is relieved of one acquirer (decrementing its use count). Once closed, a
 *  handle instance cannot be reused. <p>
 *  
 *  The {@link PageHandle} class implements the {@link AutoCloseable} interface, making it suited
 *  for use from within a try-with-resources statement. In fact, this is the recommended pattern
 *  for interacting with objects, as it guarantees that a negotiated lock is released correctly
 *  and the in-memory page is relinquished when control leaves the handle's scope. Otherwise,
 *  you must ensure that a page handle is closed (by calling {@link #close()}) when you are
 *  done with the object. <p>
 *  
 *  Additionally, a handle offers convenience methods for replacing the value of an in-memory
 *  object and for forcibly reloading the object from the storage tier. In order to use the
 *  {@link #reload()} and {@link #replace(Object)} methods, the caller must first negotiate
 *  an exclusive lock over the acquired object.
 *
 *  @param <I> ID type.
 *  @param <J> Object type.
 */
public final class PageHandle<I, J> implements AutoCloseable {
  private final Capacitor<I, J> capacitor;
  
  private final Page<I, J> page;
  
  private LockMode lockMode = LockMode.NONE;
  
  private boolean released;
  
  PageHandle(Capacitor<I, J> capacitor, Page<I, J> page) {
    this.capacitor = capacitor;
    this.page = page;
  }
  
  private void ensureOpen() {
    mustBeFalse(released, illegalState("Handle is closed"));
  }
  
  public I id() {
    return page.getId();
  }
  
  public J value() {
    ensureOpen();
    return page.getValueRef().get();
  }
  
  public LockMode getLockMode() {
    return lockMode;
  }
  
  public PageHandle<I, J> lock(LockMode lockMode) {
    ensureOpen();
    NegotiateLock.alter(page.getValueLock(), this.lockMode, lockMode);
    this.lockMode = lockMode;
    return this;
  }
  
  public J reload() throws PageLoadException, NoSuchPageException {
    final var newValue = capacitor.loadFromAccess(page.getId(), null);
    replace(newValue);
    return newValue;
  }
  
  public void replace(J newValue) {
    mustBeEqual(LockMode.EXCLUSIVE, lockMode, illegalState("Page replacement requires exclusive lock mode"));
    page.getValueRef().set(newValue);
  }
  
  @Override
  public void close() {
    if (! released) {
      lock(LockMode.NONE);
      page.release();
      released = true;
    }
  }
  
  @Override
  public String toString() {
    return PageHandle.class.getSimpleName() + " [id=" + page.getId() + ", lockMode=" + lockMode + "]";
  }
}
