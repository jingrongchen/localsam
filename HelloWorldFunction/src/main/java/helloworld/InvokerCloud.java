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
import io.pixelsdb.pixels.planner.plan.physical.input.AggregationInput;
import io.pixelsdb.pixels.planner.plan.physical.input.BroadcastJoinInput;
import io.pixelsdb.pixels.planner.plan.physical.output.AggregationOutput;
import io.pixelsdb.pixels.worker.common.BaseBroadcastJoinWorker;
import io.pixelsdb.pixels.planner.plan.physical.output.PartitionOutput;
import io.pixelsdb.pixels.common.turbo.*;
import io.pixelsdb.pixels.planner.plan.physical.input.ScanInput;
import io.pixelsdb.pixels.planner.plan.physical.input.CombinedPartitionInput;

public class InvokerCloud {
    
    public static CompletableFuture<Output> invokeCloudCombinedPartition(InvokerFactory invokerFactory,CombinedPartitionInput combinedPartitionInput) {
        return invokerFactory.getInvoker(WorkerType.COMBINED_PARTITION).invoke(combinedPartitionInput);
    }

    public static CompletableFuture<Output> invokeCloudBrocastJoin(InvokerFactory invokerFactory,BroadcastJoinInput joinInput) {
        return invokerFactory.getInvoker(WorkerType.BROADCAST_JOIN).invoke(joinInput);
    }

    public static CompletableFuture<Output> invokeCloudFusionJoinScan(InvokerFactory invokerFactory,JoinScanFusionInput joinScanInput){
        return invokerFactory.getInvoker(WorkerType.JOINSCANFUSION).invoke(joinScanInput);
    }

    public static CompletableFuture<Output> invokeCloudPartitionJoin(InvokerFactory invokerFactory,PartitionedJoinInput joinInput) {
        return invokerFactory.getInvoker(WorkerType.PARTITIONED_JOIN).invoke(joinInput);
    }

    public static CompletableFuture<Output> invokeCloudPartition(InvokerFactory invokerFactory,PartitionInput partitionInput) {
        return invokerFactory.getInvoker(WorkerType.PARTITION).invoke(partitionInput);          
    }

    public static CompletableFuture<Output> invokeCloudAggregation(InvokerFactory invokerFactory,AggregationInput aggregationInput){
            return invokerFactory.getInvoker(WorkerType.AGGREGATION).invoke(aggregationInput);
    }

    public static CompletableFuture<Output> invokeCloudScan(InvokerFactory invokerFactory,ScanInput scanInput){
        return invokerFactory.getInvoker(WorkerType.SCAN).invoke(scanInput);
    }
    
}
