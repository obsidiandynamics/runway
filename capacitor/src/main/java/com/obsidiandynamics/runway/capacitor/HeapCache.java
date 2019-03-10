package com.obsidiandynamics.runway.capacitor;

import static com.obsidiandynamics.func.Functions.*;

import java.util.*;
import java.util.concurrent.*;

/**
 *  A type of {@link TieredCache} that stores object references on the heap.
 *
 *  @param <I> ID type.
 *  @param <J> Object type.
 */
public final class HeapCache<I, J> implements TieredCache.NonTerminal<I, J> {
  private static final class Cached<I, J> {
    final I id;
    final J value;
    final long size;
    final long timestamp = System.currentTimeMillis();
    
    Cached(I id, J value, long size) {
      this.id = id;
      this.value = value;
      this.size = size;
    }
  }
  
  private TieredCache<I, J> nextTier = NullCache.getInstance();
  
  private int localTtl = 600_000;
  
  private long localCapacity = Long.MAX_VALUE;
  
  private final Map<I, Cached<I, J>> items = new ConcurrentHashMap<>();
  
  public HeapCache<I, J> withNextTier(TieredCache<I, J> nextTier) {
    this.nextTier = nextTier;
    return this;
  }
  
  public HeapCache<I, J> withLocalCapacity(long localCapacity) {
    this.localCapacity = localCapacity;
    return this;
  }
  
  public HeapCache<I, J> withLocalTtl(int localTtlMillis) {
    this.localTtl = localTtlMillis;
    return this;
  }

  @Override
  public void put(I id, J object, long size) {
    items.put(id, new Cached<>(id, object, size));
  }

  @Override
  public J pop(I id) {
    return ifEither(items.remove(id), cached -> cached.value, () -> nextTier.pop(id));
  }

  @Override
  public TieredCache<I, J> nextTier() {
    return nextTier;
  }

  @Override
  public SizeStats<I> localSize() {
    long totalSize = 0;
    final var individualStats = new HashMap<I, Long>();
    
    for (var cached : items.values()) {
      final var size = cached.size;
      individualStats.put(cached.id, size);
      totalSize += size;
    }
    
    return new SizeStats<>(totalSize, Collections.unmodifiableMap(individualStats));
  }

  @Override
  public ReclaimStats<I> reclaimLocal() {
    final var localSize = localSize();
    var currentSize = localSize.getTotalSize();
    
    var reclaimedSpace = 0L;
    final var purgedIds = new ArrayList<I>();
    
    final var timestampCutoff = System.currentTimeMillis() - localTtl;
    for (var cached : items.values()) {
      final var overCapacity = currentSize > localCapacity;
      if (overCapacity || cached.timestamp <= timestampCutoff) {
        final var id = cached.id;
        final var value = cached.value;
        final var size = cached.size;
        currentSize -= size;
        reclaimedSpace += size;
        purgedIds.add(id);
        items.remove(id);
        nextTier.put(id, value, size);
      }
      cached = null;
    }
    
    return new ReclaimStats<>(reclaimedSpace, Collections.unmodifiableList(purgedIds));
  }
  
  @Override
  public void clear() {
    items.clear();
    nextTier.clear();
  }

  @Override
  public String toString() {
    return HeapCache.class.getSimpleName() + " [localTtl=" + localTtl + 
        ", localCapacity=" + localCapacity + ", items.size=" + items.size() + 
        ", nextTier=" + nextTier + "]";
  }
}
