package com.obsidiandynamics.runway.capacitor;

/**
 *  Warm storage of objects that have been expunged from a {@link Capacitor}. <p>
 *  
 *  The intent of a tiered cache is to accelerate the loading of objects when they are
 *  requested again by the application. The capacitor will compute a digest of a cached object
 *  and may circumvent the loading of the object record if the digest of the cached object
 *  matches the persisted record.
 *
 *  @param <I> ID type.
 *  @param <J> Objects.
 */
public interface TieredCache<I, J> {
  /**
   *  Adds an object to the cache.
   *  
   *  @param id The object's ID.
   *  @param object The object.
   *  @param size The notional size of the object.
   */
  void put(I id, J object, long size);
  
  /**
   *  Retrieves the object from the cache, removing it in the process.
   *  
   *  @param id The object's ID.
   *  @return The cached object, or {@code null} if no such object exists.
   */
  J pop(I id);
  
  /**
   *  Determines whether this cache has a next tier.
   *  
   *  @return True if another tier exists after this one, or false if this is a terminal tier.
   */
  boolean hasNextTier();
  
  /**
   *  Obtains the next cache tier, assuming that this is a non-terminal cache. <p>
   *  
   *  If this is a terminal cache, this method should not be called and the implementation body can
   *  unconditionally throw an {@link UnsupportedOperationException}.
   *  
   *  @return The next cache tier.
   */
  TieredCache<I, J> nextTier();
  
  /**
   *  Obtains the size of the this cache tier.
   *  
   *  @return Local {@link SizeStats}.
   */
  SizeStats<I> localSize();
  
  /**
   *  Reclaims any lapsed objects from this cache tier.
   *  
   *  @return Outcome of this operation as a {@link ReclaimStats} object.
   */
  ReclaimStats<I> reclaimLocal();
  
  /**
   *  Clears this cache tier of all objects.
   */
  void clear();
  
  interface NonTerminal<I, J> extends TieredCache<I, J> {
    @Override
    default boolean hasNextTier() { return true; }
  }
}
