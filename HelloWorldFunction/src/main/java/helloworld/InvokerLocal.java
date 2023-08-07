package helloworld;

import java.util.concurrent.CompletableFuture;
import io.pixelsdb.pixels.worker.common.BaseAggregationWorker;
import io.pixelsdb.pixels.worker.common.BaseJoinScanFusionWorker;
import io.pixelsdb.pixels.worker.common.WorkerContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import io.pixelsdb.pixels.worker.common.WorkerMetrics;
import io.pixelsdb.pixels.planner.plan.physical.output.FusionOutput;
import io.pixelsdb.pixels.planner.plan.physical.input.JoinScanFusionInput;
import io.pixelsdb.pixels.planner.plan.physical.input.PartitionInput;
import io.pixelsdb.pixels.worker.common.BasePartitionWorker;
import io.pixelsdb.pixels.planner.plan.physical.input.PartitionedJoinInput;
import io.pixelsdb.pixels.worker.common.BasePartitionedJoinWorker;
import io.pixelsdb.pixels.planner.plan.physical.output.JoinOutput;
import io.pixelsdb.pixels.planner.plan.logical.Join;
import io.pixelsdb.pixels.planner.plan.physical.input.AggregationInput;
import io.pixelsdb.pixels.planner.plan.physical.input.BroadcastJoinInput;
import io.pixelsdb.pixels.planner.plan.physical.input.CombinedPartitionInput;
import io.pixelsdb.pixels.planner.plan.physical.output.AggregationOutput;
import io.pixelsdb.pixels.worker.common.BaseBroadcastJoinWorker;
import io.pixelsdb.pixels.worker.common.BaseCombinedPartitionWorker;
import io.pixelsdb.pixels.planner.plan.physical.output.PartitionOutput;
import io.pixelsdb.pixels.worker.common.BaseScanWorker;
import io.pixelsdb.pixels.planner.plan.physical.input.ScanInput;
import io.pixelsdb.pixels.planner.plan.physical.output.ScanOutput;
public class InvokerLocal {
    

    public static CompletableFuture<JoinOutput> invokeLocalBrocastJoin(BroadcastJoinInput joinInput) {
        WorkerMetrics workerMetrics = new WorkerMetrics();
        Logger logger = LogManager.getLogger(InvokerLocal.class);
        WorkerContext workerContext = new WorkerContext(logger, workerMetrics, "123456");
        BaseBroadcastJoinWorker baseWorker = new BaseBroadcastJoinWorker(workerContext);
        return CompletableFuture.supplyAsync(() -> {
                try {
                return baseWorker.process(joinInput);
                } catch (Exception e) {
                e.printStackTrace();
                }
                return null;
        });                
    }

    public static CompletableFuture<FusionOutput> invokeLocalFusionJoinScan(JoinScanFusionInput joinScanInput){
            WorkerMetrics workerMetrics = new WorkerMetrics();
            Logger logger = LogManager.getLogger(InvokerLocal.class);
            WorkerContext workerContext = new WorkerContext(logger, workerMetrics, "123456");
            BaseJoinScanFusionWorker baseWorker = new BaseJoinScanFusionWorker(workerContext);
            return CompletableFuture.supplyAsync(() -> {
                try {
                    return baseWorker.process(joinScanInput);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return null;
            });                
        }

    public static CompletableFuture<JoinOutput> invokeLocalPartitionJoin(PartitionedJoinInput joinInput) {
                    WorkerMetrics workerMetrics = new WorkerMetrics();
                    Logger logger = LogManager.getLogger(InvokerLocal.class);
                    WorkerContext workerContext = new WorkerContext(logger, workerMetrics, "123456");
                    BasePartitionedJoinWorker baseWorker = new BasePartitionedJoinWorker(workerContext);
                    return CompletableFuture.supplyAsync(() -> {
                            try {
                            return baseWorker.process(joinInput);
                            } catch (Exception e) {
                            e.printStackTrace();
                            }
                            return null;
    });                
    }

    public static CompletableFuture<PartitionOutput> invokeLocalPartition(PartitionInput partitionInput) {
                    WorkerMetrics workerMetrics = new WorkerMetrics();
                    Logger logger = LogManager.getLogger(InvokerLocal.class);
                    WorkerContext workerContext = new WorkerContext(logger, workerMetrics, "123456");
                    BasePartitionWorker baseWorker = new BasePartitionWorker(workerContext);
                    return CompletableFuture.supplyAsync(() -> {
                    try {
                            return baseWorker.process(partitionInput);
                    } catch (Exception e) {
                            e.printStackTrace();
                    }
                    return null;
                    });                
            }

    public static CompletableFuture<AggregationOutput> invokeLocalAggregation(AggregationInput aggregationInput){
            WorkerMetrics workerMetrics = new WorkerMetrics();
            Logger logger = LogManager.getLogger(InvokerLocal.class);
            WorkerContext workerContext = new WorkerContext(logger, workerMetrics, "123456");
            BaseAggregationWorker baseWorker = new BaseAggregationWorker(workerContext);
            return CompletableFuture.supplyAsync(() -> {
            try {
                    return baseWorker.process(aggregationInput);
            } catch (Exception e) {
                    e.printStackTrace();
            }
            return null;
            });                

    }

    public static CompletableFuture<ScanOutput> invokeLocalScan(ScanInput scanInput) {
        WorkerMetrics workerMetrics = new WorkerMetrics();
        Logger logger = LogManager.getLogger(InvokerLocal.class);
        WorkerContext workerContext = new WorkerContext(logger, workerMetrics, "123456");
        BaseScanWorker baseWorker = new BaseScanWorker(workerContext);
        return CompletableFuture.supplyAsync(() -> {
            try {
                return baseWorker.process(scanInput);
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        });                
    }

    public static CompletableFuture<JoinOutput> invokeLocalCombinedPartition(CombinedPartitionInput partitionInput) {
        WorkerMetrics workerMetrics = new WorkerMetrics();
        Logger logger = LogManager.getLogger(InvokerLocal.class);
        WorkerContext workerContext = new WorkerContext(logger, workerMetrics, "123456");
        BaseCombinedPartitionWorker baseWorker = new BaseCombinedPartitionWorker(workerContext);
        return CompletableFuture.supplyAsync(() -> {
            try {
                return baseWorker.process(partitionInput);
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        });                
    }
}
