package com.obsidiandynamics.runway.capacitor;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;

import com.obsidiandynamics.threads.*;

/**
 *  A coherent in-memory object datastore with strong concurrent access and
 *  mutation semantics. <p>
 *  
 *  A {@link Capacitor} is a low-level data structure for objects that need to be represented 
 *  coherently across a (typically slow) storage tier and an in-memory store, and where it is 
 *  preferable for the application
 *  to access and manipulate the object in memory, but occasional cache misses and loading of the
 *  object are acceptable. Additionally, changes to the object must be reflected
 *  consistently in the storage tier. A further complication is that objects may be accessed in a
 *  distributed application, with multiple processes contending over a common pool of objects. <p>
 *  
 *  A capacitor is a building block in a larger system. For this pattern to function correctly, 
 *  the  application must be organised as a distributed actor system, serialising access to 
 *  objects and maintaining locality (same objects consistently accessed by the same process). 
 *  A capacitor cannot be used
 *  when processes are accessing objects concurrently (even with distributed locking) without a
 *  guarantee of locality. Processes may lose locality of objects, through reassignment/rebalancing
 *  or cluster partitioning. This is acceptable, providing that either object locality is leased
 *  on a minimum time basis, or that the loss of locality can be reliably detected by the 
 *  application (which can then drain the capacitor). <p>
 *  
 *  Objects are accessed through a call to {@link Capacitor#acquire(Object)}, which invokes the
 *  provided {@link PageAccess} implementation — responsible for loading and unloading objects.
 *  Access is guarded by a {@link PageHandle}, which provides reentrant shared and exclusive 
 *  locking at object granularity. A {@link PageHandle} is typically used in a try-with-resources 
 *  statement, so that locks are released correctly. When an object's state is mutated, 
 *  the application  may write the change directly to the storage tier within the scope of 
 *  the handle; either before or after mutating the object, as long as the change is written 
 *  before yielding the exclusive lock. Alternatively, the application may elect not to write
 *  the change back immediately, but instead wait until the object is eventually unloaded, 
 *  writing the change then. The latter approach may not satisfy the application's safety 
 *  requirements, depending on whether the application can afford to lose the
 *  object state in-between loading and unloading of the object (which may be arbitrarily long). <p>
 *  
 *  Unloaded objects may optionally be relegated to an internal {@link TieredCache} (a no-op by
 *  default). If enabled, a tiered cache treats the object as 'potentially stale'; when loading
 *  the object the tiered cache is consulted first, and a digest of the cached object is computed
 *  via a call to {@link PageAccess#digestOf(Object)}. Rather than loading the object 
 *  unconditionally, {@link PageAccess} can compare the digest of the cached object with the
 *  persisted record, skipping the full record loading if the digests match. (Support of object
 *  digests is a prerequisite for tiered caching; caching cannot be enabled if digests are
 *  unsupported.) <p>
 *  
 *  The size of the in-memory store can be restricted by specifying one or both of a time to live 
 *  (TTL) limit and a capacity limit (in notional units). A unit is a purely arbitrary measure 
 *  that is specific to the object type; the attribution of a quantity of units to an object is 
 *  done by the {@link PageAccess} implementation. The reclamation of a capacitor's resident set
 *  is handled as a separate concern by {@link CapacitorGC}, which is responsible for periodically
 *  invoking {@link #reclaimResidents()}.
 *
 *  @param <I> ID type.
 *  @param <J> Object type.
 */
public final class Capacitor<I, J> {
  /** When the size of an object cannot be established. (Typically if the page is being loaded.) */
  private static final long UNKNOWN_SIZE = -1;
  
  private enum PageState {
    CREATED, READY, DISPOSED
  }

  private static final class PageImpl<I, J> implements Page<I, J> {
    final I id;
    final Object pageLock = new Object();
    final ReadWriteLock valueLock = new ReentrantReadWriteLock();
    PageState state = PageState.CREATED;
    final AtomicInteger useCount = new AtomicInteger();
    final AtomicReference<J> valueRef = new AtomicReference<>();
    volatile long lastUseTimestamp;

    PageImpl(I id) {
      this.id = id;
    }

    @Override
    public ReadWriteLock getValueLock() {
      return valueLock;
    }

    @Override
    public I getId() {
      return id;
    }

    @Override
    public AtomicReference<J> getValueRef() {
      return valueRef;
    }

    @Override
    public void release() {
      useCount.decrementAndGet();
      lastUseTimestamp = System.currentTimeMillis();
    }

    boolean inUse() {
      return useCount.get() > 0;
    }
  }

  private final PageAccess<I, J> access;

  private int residentTtlMillis = 60_000;

  private long residentCapacity = Long.MAX_VALUE;
  
  private TieredCache<I, J> cache = NullCache.getInstance();
  
  private String name = getClass().getSimpleName() + "@" + Integer.toHexString(hashCode());

  private final Map<I, PageImpl<I, J>> pages = new ConcurrentHashMap<>();

  public Capacitor(PageAccess<I, J> access) {
    this.access = access;
  }

  public Capacitor<I, J> withResidentTtl(int residentTtlMillis) {
    this.residentTtlMillis = residentTtlMillis;
    return this;
  }

  public Capacitor<I, J> withResidentCapacity(long residentCapacity) {
    this.residentCapacity = residentCapacity;
    return this;
  }
  
  public Capacitor<I, J> withCache(TieredCache<I, J> cache) {
    this.cache = cache;
    return this;
  }
  
  public String getName() {
    return name;
  }
  
  public Capacitor<I, J> withName(String name) {
    this.name = name;
    return this;
  }
  
  public TieredCache<I, J> getCache() {
    return cache;
  }

  public PageHandle<I, J> acquire(I id) throws PageLoadException, NoSuchPageException {
    return new PageHandle<>(this, acquirePage(id));
  }

  private PageImpl<I, J> acquirePage(I id) throws PageLoadException, NoSuchPageException {
    for (;;) {
      final var page = pages.computeIfAbsent(id, PageImpl::new);
      final var timeBeforeLock = System.currentTimeMillis(); // check time outside of the synchronized block
      synchronized (page.pageLock) {
        final var pageStatus = page.state;
        if (pageStatus != PageState.DISPOSED) {
          final var previousUseCount = page.useCount.getAndIncrement();
          if (pageStatus == PageState.CREATED) {
            J loadedValue = null;
            try {
              loadedValue = loadPage(id);
              page.valueRef.set(loadedValue);
              page.state = PageState.READY;
              page.lastUseTimestamp = System.currentTimeMillis(); // loading may have taken some time; use the latest time
              return page;
            } finally {
              if (loadedValue == null) {
                disposePage(page, UNKNOWN_SIZE);
              }
            }
          } else {
            // page is the ready state... check validity
            if (previousUseCount == 0 && timeBeforeLock - page.lastUseTimestamp >= residentTtlMillis) {
              // TTL expired with no outstanding uses, dispose the page and cycle again
              disposePage(page, access.sizeOf(page.valueRef.get()));
            } else {
              return page;
            }
          }
        }
      }
    }
  }
  
  private J loadPage(I id) throws PageLoadException, NoSuchPageException {
    final var cached = cache.pop(id);
    if (cached != null) {
      final var digest = access.digestOf(cached);
      final var loaded = loadFromAccess(id, digest);
      return loaded != null ? loaded : cached;
    } else {
      return access.load(id, null);
    }
  }
  
  J loadFromAccess(I id, Object digest) throws PageLoadException, NoSuchPageException {
    return access.load(id, digest);
  }

  private void disposePage(PageImpl<I, J> page, long size) {
    page.state = PageState.DISPOSED;
    final var id = page.id;
    final var value = page.valueRef.get();
    if (value != null) access.unload(id, value);
    if (size != UNKNOWN_SIZE && access.isCachingSupported()) {
      cache.put(id, value, size);
    }
    pages.remove(id);
  }
  
  public SizeStats<I> residentSize() {
    var totalSize = 0L;
    final var individualStats = new HashMap<I, Long>();
    for (var entry : pages.entrySet()) {
      final var page = entry.getValue();
      final var size = computeSizeIfLoaded(page);
      if (size != UNKNOWN_SIZE) {
        totalSize += size;
        final var id = entry.getKey();
        individualStats.put(id, size);
      }
    }
    return new SizeStats<>(totalSize, Collections.unmodifiableMap(individualStats));
  }

  public long sizeOf(I id) throws PageLoadException, NoSuchPageException {
    final var page = acquirePage(id);
    try {
      // after acquisition the page is guaranteed to be loaded
      return computeSizeIfLoaded(page);
    } finally {
      page.release();
    }
  }

  private long computeSizeIfLoaded(PageImpl<?, J> page) {
    final J value;
    synchronized (page.pageLock) {
      value = page.valueRef.get();
    }

    if (value != null) {
      page.valueLock.readLock().lock();
      try {
        return access.sizeOf(value);
      } finally {
        page.valueLock.readLock().unlock();
      }
    } else {
      return UNKNOWN_SIZE;
    }
  }

  public ReclaimStats<I> reclaimResidents() {
    final var residentSize = residentSize();
    var currentSize = residentSize.getTotalSize();

    var reclaimedSpace = 0L;
    final var purgedIds = new ArrayList<I>();
    final var now = System.currentTimeMillis();
    final var lastUseTimestampCutoff = now - residentTtlMillis;
    for (var entry : pages.entrySet()) {
      final var id = entry.getKey();
      final var page = entry.getValue();
      final var overCapacity = currentSize > residentCapacity;
      final var reclaimCandidate = overCapacity || page.lastUseTimestamp <= lastUseTimestampCutoff;
      if (reclaimCandidate) {
        synchronized (page.pageLock) {
          if (page.state == PageState.READY && ! page.inUse()) {
            final var precomputedSizeFromStats = residentSize.getIndividualStats().get(id);
            final var size = precomputedSizeFromStats != null ? precomputedSizeFromStats : computeSizeIfLoaded(page);
            currentSize -= size;
            reclaimedSpace += size;
            purgedIds.add(id);
            disposePage(page, size);
          }
        }
      }
    }

    return new ReclaimStats<>(reclaimedSpace, Collections.unmodifiableList(purgedIds));
  }
  
  /**
   *  Drains the capacitor, waiting for the pages to be released before forcibly unloading
   *  them.
   */
  public void drain() {
    while (! pages.isEmpty()) {
      for (var page : pages.values()) {
        synchronized (page.pageLock) {
          while (page.inUse()) Threads.sleep(1);
          disposePage(page, UNKNOWN_SIZE);
        }
      }
    }
    cache.clear();
  }

  @Override
  public String toString() {
    return Capacitor.class.getSimpleName() + " [name=" + name + ", residentTtl=" + residentTtlMillis + 
        ", residentCapacity=" + residentCapacity + ", pages.size=" + pages.size() + "]";
  }
}
