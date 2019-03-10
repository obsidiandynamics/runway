package com.obsidiandynamics.runway.capacitor;

import com.obsidiandynamics.func.*;

/**
 *  A no-op cache that acts as a terminal tier (having no further tiers).
 *
 *  @param <I> ID type.
 *  @param <J> Object type.
 */
public final class NullCache<I, J> implements TieredCache<I, J> {
  private static final NullCache<?, ?> empty = new NullCache<>();
  
  static <I, J> NullCache<I, J> getInstance() {
    return Classes.cast(empty);
  }
  
  private NullCache() {}
  
  @Override
  public void put(I id, J object, long size) {}

  @Override
  public J pop(I id) {
    return null;
  }

  @Override
  public boolean hasNextTier() {
    return false;
  }

  @Override
  public TieredCache<I, J> nextTier() {
    throw new UnsupportedOperationException(NullCache.class.getSimpleName() + " is a terminal tier");
  }

  @Override
  public SizeStats<I> localSize() {
    return SizeStats.empty();
  }

  @Override
  public ReclaimStats<I> reclaimLocal() {
    return ReclaimStats.empty();
  }
  
  @Override
  public void clear() {}
}
