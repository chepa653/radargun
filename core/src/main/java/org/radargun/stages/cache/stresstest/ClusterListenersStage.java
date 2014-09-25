package org.radargun.stages.cache.stresstest;

import org.radargun.DistStageAck;
import org.radargun.config.Property;
import org.radargun.config.Stage;
import org.radargun.traits.CacheInformation;
import org.radargun.traits.CacheListeners;
import org.radargun.traits.InjectTrait;

import java.util.Arrays;
import java.util.Collection;

/**
 * // TODO: Document this
 *
 * @author vchepeli@redhat.com
 * @since 2.0
 */
@Stage(doc = "Benchmark operations performance where cluster listeners are enabled or disabled.")
public class ClusterListenersStage extends StressTestStage {

   @Property(doc = "During stress stage, cluster listeners could be enabled. This is flag to turn them on. Default is disabled.")
   protected boolean enableClusterListeners = false;

   @InjectTrait(dependency = InjectTrait.Dependency.MANDATORY)
   private CacheInformation cacheInformation;
   @InjectTrait(dependency = InjectTrait.Dependency.MANDATORY) // could be called with infinispan70 plugin
   private CacheListeners listeners;

   @Override
   public DistStageAck executeOnSlave() {
      if (enableClusterListeners)
         registerListeners();

      return super.executeOnSlave();
   }

   protected void registerListeners() {

      if (listeners == null) {
         throw new IllegalArgumentException("Service does not support cache listeners");
      }
      Collection<CacheListeners.Type> supported = listeners.getSupportedListeners();
      if (!supported.containsAll(Arrays.asList(CacheListeners.Type.CREATED, CacheListeners.Type.EVICTED, CacheListeners.Type.REMOVED, CacheListeners.Type.UPDATED))) {
         throw new IllegalArgumentException("Service does not support required listener types; supported are: " + supported);
      }
      listeners.addCreatedListener(cacheInformation.getDefaultCacheName(), new CacheListeners.CreatedListener() {
         @Override
         public void created(Object key, Object value) {
            log.trace("Created " + key + " -> " + value);
         }
      });
      listeners.addEvictedListener(cacheInformation.getDefaultCacheName(), new CacheListeners.EvictedListener() {
         @Override
         public void evicted(Object key, Object value) {
            log.trace("Evicted " + key + " -> " + value);
         }
      });
      listeners.addRemovedListener(cacheInformation.getDefaultCacheName(), new CacheListeners.RemovedListener() {
         @Override
         public void removed(Object key, Object value) {
            log.trace("Removed " + key + " -> " + value);
         }
      });

      listeners.addUpdatedListener(cacheInformation.getDefaultCacheName(), new CacheListeners.UpdatedListener() {
         @Override
         public void updated(Object key, Object value) {
            log.trace("Updated " + key + " -> " + value);
         }
      });
   }
}
