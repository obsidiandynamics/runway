package com.obsidiandynamics.runway.capacitor;

import java.util.*;
import java.util.concurrent.*;

import com.obsidiandynamics.func.*;
import com.obsidiandynamics.worker.*;
import com.obsidiandynamics.zerolog.*;

/**
 *  Responsible for periodically reclaiming objects from one or more {@link Capacitor} instances, by
 *  invoking {@link Capacitor#reclaimResidents()} in a dedicated thread. <p>
 *  
 *  If a capacitor is equipped with a tiered cache, the GC will also drill into the cache tiers and
 *  invoke {@link TieredCache#reclaimLocal()} for each tier. <p>
 *  
 *  An optional {@link GCListener} can be registered with {@link CapacitorGC}. The listener is invoked
 *  for every capacitor and every cache tier that is collected.
 */
public final class CapacitorGC implements Joinable, Terminable {
  public final static class Options {
    int interval = 1_000;

    GCListener listener = new ZlgGCListener();

    public Options withInterval(int intervalMillis) {
      this.interval = intervalMillis;
      return this;
    }

    public Options withListener(GCListener listener) {
      this.listener = listener;
      return this;
    }
  }

  interface GCListener {
    <I> void reclaimCapacitor(Capacitor<I, ?> capacitor, ReclaimStats<I> reclaimStats);

    <I> void reclaimCache(Capacitor<I, ?> capacitor, int tier, ReclaimStats<I> reclaimStats);
  }

  public final static class ZlgGCListener implements GCListener {
    private Zlg zlg = Zlg.forDeclaringClass().get();

    private int level = LogLevel.DEBUG;

    public ZlgGCListener withZlg(Zlg zlg) {
      this.zlg = zlg;
      return this;
    }
    
    public ZlgGCListener withLevel(int level) {
      this.level = level;
      return this;
    }

    @Override
    public <I> void reclaimCapacitor(Capacitor<I, ?> capacitor, ReclaimStats<I> reclaimStats) {
      zlg
      .level(level)
      .format("%s: reclaimed %,d page(s), %,d unit(s) freed")
      .arg(capacitor::getName)
      .arg(reclaimStats.getPurgedIds()::size)
      .arg(reclaimStats::getReclaimedSpace)
      .log();
    }

    @Override
    public <I> void reclaimCache(Capacitor<I, ?> capacitor, int tier, ReclaimStats<I> reclaimStats) {
      zlg
      .level(level)
      .format("%s: reclaimed %,d page(s) from tier %d, %,d unit(s) freed")
      .arg(capacitor::getName)
      .arg(tier)
      .arg(reclaimStats.getPurgedIds()::size)
      .arg(reclaimStats::getReclaimedSpace)
      .log();
    }
  }

  private final Options options;

  private final WorkerThread thread;

  private final Set<Capacitor<?, ?>> capacitors = new CopyOnWriteArraySet<>();

  public CapacitorGC(Options options) {
    this.options = options;
    thread = WorkerThread
        .builder()
        .onCycle(this::reaperCycle)
        .withOptions(new WorkerOptions().daemon().withName(CapacitorGC.class, "reaper"))
        .buildAndStart();
  }

  private void reaperCycle(WorkerThread thread) throws InterruptedException {
    for (var capacitor : capacitors) {
      final var capacitorReclaim = capacitor.reclaimResidents();
      if (! capacitorReclaim.getPurgedIds().isEmpty()) {
        options.listener.reclaimCapacitor(capacitor, Classes.cast(capacitorReclaim));
      }

      var cache = capacitor.getCache();
      var tier = 0;
      for (;;) {
        final var cacheReclaim = cache.reclaimLocal();
        if (! cacheReclaim.getPurgedIds().isEmpty()) {
          options.listener.reclaimCache(capacitor, tier, Classes.cast(cacheReclaim));
        }
        if (cache.hasNextTier()) {
          cache = cache.nextTier();
          tier++;
        } else {
          break;
        }
      }
    }

    Thread.sleep(options.interval);
  }

  public CapacitorGC withCapacitor(Capacitor<?, ?> capacitor) {
    capacitors.add(capacitor);
    return this;
  }

  @Override
  public Joinable terminate() {
    thread.terminate();
    return this;
  }

  @Override
  public boolean join(long timeoutMillis) throws InterruptedException {
    return thread.join(timeoutMillis);
  }
}
