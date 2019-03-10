package com.obsidiandynamics.runway.capacitor;

/**
 *  Stipulates behaviour for loading and unloading objects from a persistence tier, as well
 *  as for object size and digest calculations.
 *
 *  @param <I> ID type.
 *  @param <J> Object type.
 */
public interface PageAccess<I, J> {
  /**
   *  Loads an object from the storage tier, given its unique identifier. <p>
   *  
   *  If tiered caching is supported and a cached object is available, this method will 
   *  be invoked with
   *  a non-{@code null} {@code cachedDigest}. If the object is deemed unchanged, returning
   *  {@code null} instructs the {@link Capacitor} to load the object from its tiered cache.
   *  
   *  @param id The object's ID.
   *  @param cachedDigest The cached digest (if caching is enabled and a cached object exists).
   *  @return The loaded object, or {@code null} if the persisted record agrees with the cached digest.
   *  @throws PageLoadException If an error occurred while loading the object.
   *  @throws NoSuchPageException If no object for the given ID exists.
   */
  J load(I id, Object cachedDigest) throws PageLoadException, NoSuchPageException;
  
  /**
   *  Optionally overridden to provide object unloading behaviour. <p>
   *  
   *  Invoking this method presents an opportunity for the {@link PageAccess} implementation
   *  to persist the state of the object just prior to it being flushed from the
   *  {@link Capacitor}. The late persistence of an object in this manner should only be used
   *  if the application can tolerate loss of changes to an object's state in-between
   *  loading and unloading; otherwise, every change to an object must be persisted at the
   *  point when the change is made (prior to relinquishing the exclusive lock).
   *  
   *  @param id The ID of the object to unload.
   *  @param object The current in-memory representation of the object being unloaded.
   */
  default void unload(I id, J object) {}
  
  /**
   *  Calculates the notional size of the given object. This size metric is used by the
   *  {@link Capacitor} class to prune its in-memory resident set. <p>
   *  
   *  The size metric is purely arbitrary and implementation specific. The only requirement
   *  is consistency between the result of the {@link #sizeOf(Object)} method and
   *  the capacitor's resident capacity. For example, if the average size of an object is
   *  1,000 notional units, and the capacity limit is 100,000 units, then on average 100
   *  objects will be retained by the capacitor before pruning occurs.
   *  
   *  @param object The object whose size to measure.
   *  @return The size of the object.
   */
  long sizeOf(J object);
  
  /**
   *  Provides a digest of the given object, used when loading the object from the tiered cache. <p>
   *  
   *  The digest should attempt to capture the object's state unambiguously, so as to avoid collisions
   *  (two non-identical objects yielding the same digest). If a unique digest cannot be derived, and
   *  loading a stale object cannot be tolerated, then caching should be disabled for safety. <p>
   *  
   *  This method must be implemented if caching is supported (i.e. {@link #isCachingSupported()}
   *  returns {@code true}). Otherwise, this method will never be called and the implementation can
   *  unconditionally throw an {@link UnsupportedOperationException} from its body.
   *  
   *  @param object The object to analyse.
   *  @return The object's digest.
   */
  Object digestOf(J object);
  
  /**
   *  Indicates whether or not tiered caching is supported. <p>
   *  
   *  It is recommended to derive from {@link PageAccess.Caching} and {@link PageAccess.NonCaching}
   *  directly, which improve readability by reducing boilerplate code.
   *  
   *  @return True if caching is supported.
   */
  boolean isCachingSupported();
  
  interface Caching<I, J> extends PageAccess<I, J> {
    @Override
    default boolean isCachingSupported() { return true; }
  }
  
  interface NonCaching<I, J> extends PageAccess<I, J> {
    @Override
    default boolean isCachingSupported() { return false; }
    
    @Override 
    default Object digestOf(J object) { throw new UnsupportedOperationException("Non-caching page access"); }
  }
}
