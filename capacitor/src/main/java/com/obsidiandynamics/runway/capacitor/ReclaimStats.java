package com.obsidiandynamics.runway.capacitor;

import java.util.*;

import org.apache.commons.lang3.builder.*;

import com.obsidiandynamics.func.*;

/**
 *  Information about objects that were reclaimed from a {@link Capacitor} or a {@link TieredCache}.
 *
 *  @param <I> ID type.
 */
public final class ReclaimStats<I> {
  private static final ReclaimStats<?> empty = new ReclaimStats<>(0, Collections.emptyList());
  
  static <I> ReclaimStats<I> empty() {
    return Classes.cast(empty);
  }
  
  private final long reclaimedSpace;
  
  private final List<I> purgedIds;
  
  ReclaimStats(long reclaimedSpace, List<I> purgedIds) {
    this.reclaimedSpace = reclaimedSpace;
    this.purgedIds = purgedIds;
  }

  public long getReclaimedSpace() {
    return reclaimedSpace;
  }

  public List<I> getPurgedIds() {
    return purgedIds;
  }
  
  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(reclaimedSpace).append(purgedIds).toHashCode();
  }
  
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (obj instanceof ReclaimStats) {
      final ReclaimStats<?> that = (ReclaimStats<?>) obj;
      return new EqualsBuilder()
          .append(reclaimedSpace, that.reclaimedSpace)
          .append(purgedIds, that.purgedIds)
          .isEquals();
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    return ReclaimStats.class.getSimpleName() + " [reclaimedSpace=" + reclaimedSpace + 
        ", purgedIds.size=" + purgedIds.size() + "]";
  }
}