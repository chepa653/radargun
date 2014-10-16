package org.radargun.stages.cache.listeners.cluster;

import org.radargun.DistStageAck;
import org.radargun.StageResult;
import org.radargun.config.Property;
import org.radargun.config.Stage;
import org.radargun.reporting.Report;
import org.radargun.stages.AbstractDistStage;
import org.radargun.state.SlaveState;
import org.radargun.stats.DefaultOperationStats;
import org.radargun.stats.Statistics;
import org.radargun.stats.SynchronizedStatistics;
import org.radargun.traits.CacheListeners;
import org.radargun.traits.InjectTrait;
import org.radargun.utils.TimeConverter;

import java.util.Arrays;
import java.util.List;

import static org.radargun.traits.CacheListeners.*;

/**
 * Run this stage if you want to compare performance with enabled/disabled cluster listenersTrait
 *
 * @author vchepeli@redhat.com
 * @since 2.0
 */
@Stage(doc = "Benchmark operations performance where cluster listenersTrait are enabled or disabled.")
public class RegisterListenersStage extends AbstractDistStage {

   @Property(doc = "Simulate some work executed on cluster listener. This is flag is used in combination with sleep-time flag. Default is false.")
   protected boolean simulateWork = false;

   @Property(doc = "Use sleep time to simulate some work on listener. This flag is used in combination with simulate-work flag. Default is 5 ms.", converter = TimeConverter.class)
   protected long sleepTime = 5;

   @Property(doc = "Before stress stage, cluster listeners would be enabled. This is flag to turn them on. Default is false.")
   protected boolean registerListeners = false;

   @Property(doc = "Before stress stage, cluster listeners would be disabled. This is flag to turn them off. Default is false.")
   protected boolean unregisterListeners = false;

   @InjectTrait // with infinispan70 plugin
   private CacheListeners listenersTrait;

   private SynchronizedStatistics statistics = new SynchronizedStatistics(new DefaultOperationStats());

   @Override
   public DistStageAck executeOnSlave() {
      if (registerListeners){
         initListenersOnSlave(slaveState);
         registerListeners();
      }

      if (unregisterListeners) {
         unregisterListeners();
      }

      return successfulResponse();
   }

   @Override
   public StageResult processAckOnMaster(List<DistStageAck> acks) {
      StageResult result = super.processAckOnMaster(acks);
      if (result.isError()) return result;

      Report.Test test = getTest("FAKE_TEST");
      int testIteration = test == null ? 0 : test.getIterations().size();
      Statistics aggregated = statistics.copy();
      for (DistStageAck ack : acks) {
         if (test != null) {
            test.addStatistics(++testIteration, ack.getSlaveIndex(), Arrays.asList(aggregated));
         }
      }

      return StageResult.SUCCESS;
   }


   protected Report.Test getTest(String testName) {
      if (testName == null || testName.isEmpty()) {
         log.warn("No test name - results are not recorded");
         return null;
      } else if (testName.equalsIgnoreCase("warmup")) {
         log.info("This test was executed as a warmup");
         return null;
      } else {
         Report report = masterState.getReport();
         Report.Test test = report.getOrCreateTest(testName, true);
         return test;
      }
   }

   private void initListenersOnSlave(SlaveState slaveState) {
      CreatedListener createdListener = new CreatedListener() {
         @Override
         public void created(Object key, Object value) {
            simulateWorkOnListener();
            statistics.registerRequest(0, Operations.CREATED.op());
            log.trace("Created " + key + " -> " + value);
         }
      };
      slaveState.put(Name.CREATED.sName(), createdListener);


      EvictedListener evictedListener = new EvictedListener() {
         @Override
         public void evicted(Object key, Object value) {
            simulateWorkOnListener();
            statistics.registerRequest(0, Operations.EVICTED.op());
            log.trace("Evicted " + key + " -> " + value);
         }
      };
      slaveState.put(Name.EVICTED.sName(), evictedListener);


      RemovedListener removedListener = new RemovedListener() {
         @Override
         public void removed(Object key, Object value) {
            simulateWorkOnListener();
            statistics.registerRequest(0, Operations.REMOVED.op());
            log.trace("Removed " + key + " -> " + value);
         }
      };
      slaveState.put(Name.REMOVED.sName(), removedListener);


      UpdatedListener updatedListener = new UpdatedListener() {
         @Override
         public void updated(Object key, Object value) {
            simulateWorkOnListener();
            statistics.registerRequest(0, Operations.UPDATED.op());
            log.trace("Updated " + key + " -> " + value);
         }
      };
      slaveState.put(Name.UPDATED.sName(), updatedListener);

      ExpiredListener expiredListener = new ExpiredListener() {
         @Override
         public void expired(Object key, Object value) {
            simulateWorkOnListener();
            statistics.registerRequest(0, Operations.EXPIRED.op());
            log.trace("Expired " + key + " -> " + value);
         }
      };
      slaveState.put(Name.EXPIRED.sName(), expiredListener);

      slaveState.put("RegisterListenersStage.STATS", statistics);
   }

   public void registerListeners() {
      CreatedListener createdListener = (CreatedListener) slaveState.get(Name.CREATED.sName());
      if (createdListener != null && isSupported(Type.CREATED)) {
         listenersTrait.addCreatedListener(null, createdListener);
      }
      EvictedListener evictedListener = (EvictedListener) slaveState.get(Name.EVICTED.sName());
      if (evictedListener != null && isSupported(Type.EVICTED)) {
         listenersTrait.addEvictedListener(null, evictedListener);
      }
      RemovedListener removedListener = (RemovedListener) slaveState.get(Name.REMOVED.sName());
      if (removedListener != null && isSupported(Type.REMOVED)) {
         listenersTrait.addRemovedListener(null, removedListener);
      }
      UpdatedListener updatedListener = (UpdatedListener) slaveState.get(Name.UPDATED.sName());
      if (updatedListener != null && isSupported(Type.UPDATED)) {
         listenersTrait.addUpdatedListener(null, updatedListener);
      }
      ExpiredListener expiredListener = (ExpiredListener) slaveState.get(Name.EXPIRED.sName());
      if (expiredListener != null && isSupported(Type.EXPIRED)) {
         listenersTrait.addExpiredListener(null, expiredListener);
      }
   }

   public void unregisterListeners() {
      CreatedListener createdListener = (CreatedListener) slaveState.get(Name.CREATED.sName());
      if (createdListener != null && isSupported(Type.CREATED)) {
         listenersTrait.removeCreatedListener(null, createdListener);
      }
      EvictedListener evictedListener = (EvictedListener) slaveState.get(Name.EVICTED.sName());
      if (evictedListener != null && isSupported(Type.EVICTED)) {
         listenersTrait.removeEvictedListener(null, evictedListener);
      }
      RemovedListener removedListener = (RemovedListener) slaveState.get(Name.REMOVED.sName());
      if (removedListener != null && isSupported(Type.REMOVED)) {
         listenersTrait.removeRemovedListener(null, removedListener);
      }
      UpdatedListener updatedListener = (UpdatedListener) slaveState.get(Name.UPDATED.sName());
      if (updatedListener != null && isSupported(Type.UPDATED)) {
         listenersTrait.removeUpdatedListener(null, updatedListener);
      }
      ExpiredListener expiredListener = (ExpiredListener) slaveState.get(Name.EXPIRED.sName());
      if (expiredListener != null && isSupported(Type.EXPIRED)) {
         listenersTrait.removeExpiredListener(null, expiredListener);
      }
   }

   private boolean isSupported(Type type) {
      if (listenersTrait == null) {
         throw new IllegalArgumentException("Service does not support cache listeners");
      }
      return listenersTrait.getSupportedListeners().contains(type);
   }

   private void simulateWorkOnListener() {
      if (simulateWork) {
         try {
            Thread.sleep(sleepTime);
         } catch (InterruptedException e) {
            e.printStackTrace();
         }
      }
   }
}