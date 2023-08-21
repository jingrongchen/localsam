package helloworld;

import org.junit.Test;

import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.turbo.InvokerFactory;
import io.pixelsdb.pixels.common.turbo.WorkerType;
import io.pixelsdb.pixels.planner.plan.physical.domain.*;
import io.pixelsdb.pixels.planner.plan.physical.input.ThreadScanInput;
import io.pixelsdb.pixels.planner.plan.physical.input.ScanInput;
import java.util.Arrays;
import java.util.ArrayList;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.lang.Math;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.CloudFrontEvent.CF;

import io.pixelsdb.pixels.common.turbo.Output;
import io.pixelsdb.pixels.worker.common.BaseThreadScanWorker;
import io.pixelsdb.pixels.worker.common.BaseAggregationWorker;
import io.pixelsdb.pixels.worker.common.BaseJoinScanFusionWorker;
import io.pixelsdb.pixels.worker.common.WorkerContext;
import io.pixelsdb.pixels.worker.common.WorkerContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import io.pixelsdb.pixels.worker.common.WorkerMetrics;

import org.apache.jasper.tagplugins.jstl.core.Out;
import org.apache.logging.log4j.LogManager;
import io.pixelsdb.pixels.worker.common.BaseScanWorker;
import com.alibaba.fastjson.JSON;
import io.pixelsdb.pixels.executor.aggregation.FunctionType;
import java.util.HashMap;
import io.pixelsdb.pixels.planner.plan.physical.output.ScanOutput;
import io.pixelsdb.pixels.planner.plan.logical.operation.LogicalAggregate;
import io.pixelsdb.pixels.planner.plan.logical.operation.LogicalProject;
import io.pixelsdb.pixels.planner.plan.logical.operation.ScanpipeInfo;
import io.pixelsdb.pixels.planner.plan.physical.output.FusionOutput;

import io.pixelsdb.pixels.planner.plan.physical.input.JoinScanFusionInput;
import io.pixelsdb.pixels.planner.plan.physical.input.PartitionInput;
import io.pixelsdb.pixels.executor.join.JoinType;
import io.pixelsdb.pixels.executor.predicate.TableScanFilter;
import io.pixelsdb.pixels.planner.plan.physical.output.FusionOutput;
import io.pixelsdb.pixels.worker.common.BasePartitionWorker;
import java.util.HashSet;
import java.util.Set;
import io.pixelsdb.pixels.planner.plan.physical.input.PartitionedJoinInput;
import io.pixelsdb.pixels.worker.common.BasePartitionedJoinWorker;
import io.pixelsdb.pixels.planner.plan.physical.output.JoinOutput;
import io.pixelsdb.pixels.planner.plan.physical.input.AggregationInput;
import io.pixelsdb.pixels.planner.plan.physical.input.BroadcastJoinInput;
import io.pixelsdb.pixels.planner.plan.physical.input.CombinedPartitionInput;
import io.pixelsdb.pixels.planner.plan.physical.output.AggregationOutput;
import io.pixelsdb.pixels.worker.common.BaseBroadcastJoinWorker;
import io.pixelsdb.pixels.planner.plan.physical.output.PartitionOutput;

import org.apache.commons.lang3.tuple.ImmutablePair;
// import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.hadoop.thirdparty.org.checkerframework.checker.units.qual.A;
import org.apache.hadoop.util.hash.Hash;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.fastjson.annotation.JSONField;
import com.alibaba.fastjson.annotation.JSONType;
import io.pixelsdb.pixels.core.utils.Bitmap;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import io.pixelsdb.pixels.executor.predicate.ColumnFilter;
import io.pixelsdb.pixels.core.TypeDescription;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import io.pixelsdb.pixels.executor.predicate.Filter;
import io.pixelsdb.pixels.executor.predicate.Range;


public class CombinePartitionTest {
    
    @Test
    public void testCombinedPartition() {
        String customerFilter = "{\"schemaName\":\"tpch\",\"tableName\":\"customer\",\"columnFilters\":{}}";
        PartitionInput input = new PartitionInput();
        input.setTransId(123456);
        ScanTableInfo tableInfo = new ScanTableInfo();
        tableInfo.setTableName("customer");
        tableInfo.setInputSplits(Arrays.asList(
                    new InputSplit(Arrays.asList(new InputInfo("s3://jingrong-lambda-test/tpch/customer/v-0-ordered/20230801082051_0.pxl", 0, -1)))));
        
        tableInfo.setFilter(customerFilter);
        tableInfo.setBase(true);
        // tableInfo.setColumnsToRead(new String[] { "p_partkey","p_name"});
        tableInfo.setColumnsToRead(new String[] { "c_custkey","c_name"});
        tableInfo.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null, null));
        input.setTableInfo(tableInfo);
        input.setProjection(new boolean[] { true, true });
        PartitionInfo partitionInfo = new PartitionInfo();
        partitionInfo.setNumPartition(2);
        partitionInfo.setKeyColumnIds(new int[] {0});
        input.setPartitionInfo(partitionInfo);
        input.setOutput(new OutputInfo("jingrong-lambda-test/unit_tests/testcombinepartition/customer_partition/Part_0",
        new StorageInfo(Storage.Scheme.s3, null, null, null, null), false));
        InvokerLocal invokerLocal= new InvokerLocal();
        

        String ordersFilter = "{\"schemaName\":\"tpch\",\"tableName\":\"orders\",\"columnFilters\":{}}";
        CombinedPartitionInput input2 = new CombinedPartitionInput();
        input2.setTransId(123456);
        ScanTableInfo tableInfo2 = new ScanTableInfo();
        tableInfo2.setTableName("orders");
        tableInfo2.setInputSplits(Arrays.asList(
                    new InputSplit(Arrays.asList(new InputInfo("s3://jingrong-lambda-test/tpch/orders/v-0-ordered/20230801082419_47.pxl", 0, -1))),
                    new InputSplit(Arrays.asList(new InputInfo("s3://jingrong-lambda-test/tpch/orders/v-0-ordered/20230801082422_48.pxl", 0, -1)))
                    ));
        tableInfo2.setFilter(ordersFilter);
        tableInfo2.setBase(true);
        tableInfo2.setColumnsToRead(new String[] { "o_orderkey","o_custkey","o_orderdate","o_totalprice"});
        tableInfo2.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null, null));
        input2.setTableInfo(tableInfo2);
        input2.setProjection(new boolean[] { true, true, true, true });
        PartitionInfo partitionInfo2 = new PartitionInfo();
        partitionInfo2.setNumPartition(2);
        partitionInfo2.setKeyColumnIds(new int[] {1});
        input2.setPartitionInfo(partitionInfo2);
        input2.setOutput(new OutputInfo("jingrong-lambda-test/unit_tests/testcombinepartition/orders_partition/Part_0",
        new StorageInfo(Storage.Scheme.s3, null, null, null, null), false));
        
        //join test
        PartitionedTableInfo leftTableInfo = new PartitionedTableInfo();
        leftTableInfo.setTableName("customer");
        leftTableInfo.setColumnsToRead(new String[]{"c_custkey", "c_name"});
        leftTableInfo.setKeyColumnIds(new int[]{0});
        leftTableInfo.setInputFiles(Arrays.asList("s3://jingrong-lambda-test/unit_tests/intermediate_result/customer_partition/Part_0"));
        leftTableInfo.setParallelism(2);
        leftTableInfo.setBase(false);
        leftTableInfo.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null, null));
        input2.setSmallTable(leftTableInfo);

        PartitionedTableInfo rightTableInfo = new PartitionedTableInfo();
        rightTableInfo.setTableName("orders");
        rightTableInfo.setColumnsToRead(new String[]{"o_orderkey","o_custkey","o_orderdate","o_totalprice"});
        rightTableInfo.setKeyColumnIds(new int[]{1});
        rightTableInfo.setInputFiles(Arrays.asList("s3://jingrong-lambda-test/unit_tests/intermediate_result/orders_partition/Part_0"));
        rightTableInfo.setParallelism(20);
        rightTableInfo.setBase(false);
        rightTableInfo.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null, null));
        input2.setLargeTable(rightTableInfo);

        List<Integer> hashValues = new ArrayList<Integer>();
        for (int i = 0 ; i < 1; ++i)
        {
            hashValues.add(i);
        }

        PartitionedJoinInfo joinInfo = new PartitionedJoinInfo();
        joinInfo.setJoinType(JoinType.EQUI_INNER);
        joinInfo.setNumPartition(2);
        joinInfo.setHashValues(hashValues);
        // joinInfo.setHashValues(new ArrayList<Integer>(hashValues));
        joinInfo.setSmallColumnAlias(new String[]{"c_custkey", "c_name"});
        joinInfo.setLargeColumnAlias(new String[]{"o_orderkey","o_custkey","o_orderdate","o_totalprice"});
        joinInfo.setSmallProjection(new boolean[]{true, true});
        joinInfo.setLargeProjection(new boolean[]{true, true, true, true});

        joinInfo.setPostPartition(false);
        // joinInfo.setPostPartitionInfo(new PartitionInfo(new int[] {2}, 20));
        input2.setJoinInfo(joinInfo);

        input2.setMultiOutput(new MultiOutputInfo("jingrong-lambda-test/unit_tests/testcombinepartition/customer_orders_partitioned/",
        new StorageInfo(Storage.Scheme.s3, null, null, null, null),
        false, Arrays.asList("partitioned_join_customer_orders")));

        // List<Integer> localhashValues = new ArrayList<Integer>();
        // localhashValues.add(0);
        // input2.setLocalHashedPartitionIds(localhashValues);
        input2.setIntermideatePath("s3://jingrong-lambda-test/unit_tests/testcombinepartition/");
        input2.setBigTableName("orders");
        input2.setSmallTableName("customer");
        input2.setSmallTablePartitionCount(1);
        input2.setBigTablePartitionCount(3);

        try {
            Output output = invokerLocal.invokeLocalPartition(input).get();
            JoinOutput output2 = invokerLocal.invokeLocalCombinedPartition(input2).get();
            System.out.println(JSON.toJSONString(output));
            System.out.println(JSON.toJSONString(output2));
        } catch (Exception e) {
            System.out.println("Stage one failed!");
            return;
        }

}


    @Test
    public void testAddpartition(){
        String ordersFilter = "{\"schemaName\":\"tpch\",\"tableName\":\"orders\",\"columnFilters\":{}}";
        PartitionInput input2 = new CombinedPartitionInput();
        input2.setTransId(123456);
        ScanTableInfo tableInfo2 = new ScanTableInfo();
        tableInfo2.setTableName("orders");
        tableInfo2.setInputSplits(Arrays.asList(
                    new InputSplit(Arrays.asList(new InputInfo("s3://jingrong-lambda-test/tpch/orders/v-0-ordered/20230801082419_47.pxl", 0, -1))),
                    new InputSplit(Arrays.asList(new InputInfo("s3://jingrong-lambda-test/tpch/orders/v-0-ordered/20230801082422_48.pxl", 0, -1)))
                    ));
        tableInfo2.setFilter(ordersFilter);
        tableInfo2.setBase(true);
        tableInfo2.setColumnsToRead(new String[] { "o_orderkey","o_custkey","o_orderdate","o_totalprice"});
        tableInfo2.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null, null));
        input2.setTableInfo(tableInfo2);
        input2.setProjection(new boolean[] { true, true, true, true });
        PartitionInfo partitionInfo2 = new PartitionInfo();
        partitionInfo2.setNumPartition(2);
        partitionInfo2.setKeyColumnIds(new int[] {1});
        input2.setPartitionInfo(partitionInfo2);
        input2.setOutput(new OutputInfo("jingrong-lambda-test/unit_tests/testcombinepartition/orders_partition/Part_1",
        new StorageInfo(Storage.Scheme.s3, null, null, null, null), false));
        InvokerLocal invokerLocal= new InvokerLocal();
        try {
            Output output2 = invokerLocal.invokeLocalPartition(input2).get();
            System.out.println(JSON.toJSONString(output2));
        } catch (Exception e) {
            System.out.println("Stage one failed!");
            return;
        }
        // invokerLocal.invokeLocalPartition(input2).get();

    }
}
