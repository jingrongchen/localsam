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
public class InpipelineTest {


    @Test
    public void colHashJoinTest(){

        int numOfPartition = 80;
        int HashJoinCFnumber= 80;
        int numOfPostPartition = 80;
        boolean islocal = true;
        List<Integer> hashvalue = new ArrayList<Integer>();
        for (int i=0;i<numOfPostPartition;i++){
                hashvalue.add(i);
        }
        HashMap<String, List<String>> stage3Files = Common.getTableToString("s3://jingrong-lambda-test/unit_tests/intermediate_result/",Arrays.asList("customer_orders_partitionjoin","lineitem_partition"));

        List<CompletableFuture<Output>> Cloudfutures = new ArrayList<CompletableFuture<Output>>();                    
        // customer and orders join result join lineitem partitions
        List<CompletableFuture<JoinOutput>> futures = new ArrayList<CompletableFuture<JoinOutput>>();
        List<CompletableFuture<AggregationOutput>> aggFutures = new ArrayList<CompletableFuture<AggregationOutput>>();
        List<CompletableFuture<PartitionOutput>> partitionFutures = new ArrayList<CompletableFuture<PartitionOutput>>();
        //second stage input files
        List<String> secondStageFiles = new ArrayList<String>();

        List<JoinOutput> joinOutput = new ArrayList<JoinOutput>();
        // List<AggregationOutput> aggregationOutput = new ArrayList<AggregationOutput>();

        PartitionedJoinInput joinInput = new PartitionedJoinInput();
        joinInput.setTransId(123456);
        PartitionedTableInfo leftTableInfo = new PartitionedTableInfo();
        leftTableInfo.setTableName("customer_orders");
        leftTableInfo.setColumnsToRead(new String[]{"c_custkey","c_name","o_orderkey", "o_custkey", "o_orderdate","o_totalprice"});
        leftTableInfo.setKeyColumnIds(new int[]{2});
        
        // kind: LONG
        // name: "c_custkey"

        // kind: VARCHAR
        // name: "c_name"
        // maximumLength: 25

        // kind: LONG
        // name: "o_orderkey"

        // kind: LONG
        // name: "o_custkey"

        // kind: DATE
        // name: "o_orderdate"

        // kind: DECIMAL
        // name: "o_totalprice"
        // precision: 15

        leftTableInfo.setInputFiles(stage3Files.get("customer_orders_partitionjoin"));
        leftTableInfo.setParallelism(2);
        leftTableInfo.setBase(false);
        leftTableInfo.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null, null));
        joinInput.setSmallTable(leftTableInfo);
        //filter the result of aggregation

        PartitionedTableInfo rightTableInfo = new PartitionedTableInfo();
        rightTableInfo.setTableName("lineitem_partitioned");
        rightTableInfo.setColumnsToRead(new String[]{"l_orderkey", "l_quantity"});
        rightTableInfo.setKeyColumnIds(new int[]{0});

        // kind: LONG
        // name: "l_orderkey"

        // kind: DECIMAL
        // name: "l_quantity"

        rightTableInfo.setInputFiles(stage3Files.get("lineitem_partition"));
        rightTableInfo.setParallelism(8);
        rightTableInfo.setBase(false);
        rightTableInfo.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null, null));
        joinInput.setLargeTable(rightTableInfo);

        PartitionedJoinInfo joinInfo = new PartitionedJoinInfo();
        joinInfo.setJoinType(JoinType.EQUI_INNER);
        joinInfo.setNumPartition(numOfPartition);
        joinInfo.setSmallColumnAlias(new String[]{"c_custkey","c_name","o_orderkey", "o_custkey", "o_orderdate", "o_totalprice"});
        joinInfo.setLargeColumnAlias(new String[]{"l_orderkey", "l_quantity"});
        
        joinInfo.setSmallProjection(new boolean[]{true, true, true, true, true, true});
        joinInfo.setLargeProjection(new boolean[]{true, true});
        joinInfo.setPostPartition(true);
        joinInfo.setPostPartitionInfo(new PartitionInfo(new int[] {6}, numOfPostPartition));
    
        int cfPerhash = (int)Math.ceil((double)hashvalue.size()/(double)HashJoinCFnumber);
        for (int cf_num=0;cf_num<1;cf_num++){
                List<Integer> local_hashvalue = new ArrayList<Integer>();
                for (int hash_num=0;hash_num<cfPerhash;hash_num++){
                        local_hashvalue.add(hashvalue.get(cf_num*cfPerhash+hash_num));
                }
                System.out.println("hashvalue: " + local_hashvalue);

                joinInfo.setHashValues(local_hashvalue);
                joinInput.setJoinInfo(joinInfo);
                joinInput.setOutput(new MultiOutputInfo("jingrong-lambda-test/unit_tests/intermediate_result/customer_orders_lineitem_partitionjoin/",
                new StorageInfo(Storage.Scheme.s3, null, null, null, null),
                false, Arrays.asList("part_"+cf_num))); // force one file currently
                secondStageFiles.add("jingrong-lambda-test/unit_tests/intermediate_result/customer_orders_lineitem_partitionjoin/part_"+cf_num);

                if(islocal){
                        futures.add(InvokerLocal.invokeLocalPartitionJoin(joinInput));
                }
                else{
                        Cloudfutures.add(InvokerCloud.invokeCloudPartitionJoin(InvokerFactory.Instance(),joinInput));
                }
        }

        for(int i=0;i<futures.size();i++){
                try{
                    JoinOutput result = futures.get(i).get();
                    System.out.println(JSON.toJSONString(result));
                }catch(Exception e){
                        e.printStackTrace();
                }
        }

    }


}
