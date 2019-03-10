package com.obsidiandynamics.runway.capacitor;

import com.obsidiandynamics.yconf.*;

@Y
public final class CapacitorConfig {
  /** The resident set TTL, in milliseconds. */
  @YInject
  private int residentTtl = 60_000;
  
  /** The resident capacity, in units. */
  @YInject
  private long residentCapacity = Long.MAX_VALUE;

  /** The cache TTL, in milliseconds. */
  @YInject
  private int cacheTtl = 600_000;

  /** The cache capacity, in units. */
  @YInject
  private long cacheCapacity = Long.MAX_VALUE;

  public int getResidentTtl() {
    return residentTtl;
  }

  public CapacitorConfig withResidentTtl(int residentTtlMillis) {
    this.residentTtl = residentTtlMillis;
    return this;
  }

  public long getResidentCapacity() {
    return residentCapacity;
  }

  public CapacitorConfig withResidentCapacity(long residentCapacity) {
    this.residentCapacity = residentCapacity;
    return this;
  }

  public int getCacheTtl() {
    return cacheTtl;
  }

  public CapacitorConfig withCacheTtl(int cacheTtlMillis) {
    this.cacheTtl = cacheTtlMillis;
    return this;
  }

  public long getCacheCapacity() {
    return cacheCapacity;
  }

  public CapacitorConfig withCacheCapacity(long cacheCapacity) {
    this.cacheCapacity = cacheCapacity;
    return this;
  }

  @Override
  public String toString() {
    return CapacitorConfig.class.getSimpleName() + " [residentTtl=" + residentTtl + ", residentCapacity="
           + residentCapacity + ", cacheTtl=" + cacheTtl + ", cacheCapacity=" + cacheCapacity
           + "]";
  }
}
