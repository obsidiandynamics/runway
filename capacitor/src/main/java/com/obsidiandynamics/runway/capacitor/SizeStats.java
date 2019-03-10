package com.obsidiandynamics.runway.capacitor;

import java.util.*;

import org.apache.commons.lang3.builder.*;

import com.obsidiandynamics.func.*;

/**
 *  Information on the size of the objects resident in either a {@link Capacitor} or a
 *  {@link TieredCache}.
 *
 *  @param <I> ID type.
 */
public final class SizeStats<I> {
  private static final SizeStats<?> empty = new SizeStats<>(0, Collections.emptyMap());
  
  static <I> SizeStats<I> empty() {
    return Classes.cast(empty);
  }
  
  private final long totalSize;
  
  private final Map<I, Long> individualStats;
  
  SizeStats(long totalSize, Map<I, Long> individualStats) {
    this.totalSize = totalSize;
    this.individualStats = individualStats;
  }

  public long getTotalSize() {
    return totalSize;
  }

  public Map<I, Long> getIndividualStats() {
    return individualStats;
  }
  
  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(totalSize).append(individualStats).toHashCode();
  }
  
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (obj instanceof SizeStats) {
      final SizeStats<?> that = (SizeStats<?>) obj;
      return new EqualsBuilder()
          .append(totalSize, that.totalSize)
          .append(individualStats, that.individualStats)
          .isEquals();
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    return SizeStats.class.getSimpleName() + " [totalSize=" + totalSize + 
        ", individualStats.size=" + individualStats.size() + "]";
  }
}