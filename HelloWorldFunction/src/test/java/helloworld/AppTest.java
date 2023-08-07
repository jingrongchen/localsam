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


public class AppTest {

        public ImmutableTriple<Boolean, List<PartitionOutput>,List<FusionOutput>> FirstStage(boolean local,boolean enablePartition,boolean enableScan,HashMap<String, List<InputSplit>> tableToInputSplits,int filePerCf,int scanpartition,int numOfPartition) {
                
                List<CompletableFuture<PartitionOutput>> partitionFuture = new ArrayList<CompletableFuture<PartitionOutput>>();
                List<CompletableFuture<FusionOutput>> futures = new ArrayList<CompletableFuture<FusionOutput>>();
                List<CompletableFuture<Output>> CloudFuture = new ArrayList<CompletableFuture<Output>>();
                List<PartitionOutput> partitionOutputs = new ArrayList<PartitionOutput>();
                List<FusionOutput> fusionOutputs = new ArrayList<FusionOutput>();

                /**
                 * partition customer prepare
                 */
                String customerFilter = "{\"schemaName\":\"tpch\",\"tableName\":\"customer\",\"columnFilters\":{}}";
                PartitionInput input = new PartitionInput();
                input.setTransId(123456);
                ScanTableInfo tableInfo = new ScanTableInfo();
                tableInfo.setTableName("customer");

                int c_NumOfCF = (int)Math.ceil((double)tableToInputSplits.get("customer").size()/(double)filePerCf);
                System.out.println("total numof cf for customer: " + c_NumOfCF);
                // int c_NumOfFile = (int)Math.ceil((double)tableToInputSplits.get("customer").size()/(double)CFnumber);
                // System.out.println("total size of customer table: "+tableToInputSplits.get("customer").size()+" and each cf deal with : " + c_NumOfFile + " customer files");
                tableInfo.setFilter(customerFilter);
                tableInfo.setBase(true);
                tableInfo.setColumnsToRead(new String[] { "c_custkey","c_name"});
                tableInfo.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null, null));
                input.setTableInfo(tableInfo);
                input.setProjection(new boolean[] { true, true });
                PartitionInfo partitionInfo = new PartitionInfo();
                partitionInfo.setNumPartition(numOfPartition);
                partitionInfo.setKeyColumnIds(new int[] {0});
                //***********************************************************/


                /**
                 * partition orders prepare
                 */
                String ordersFilter = "{\"schemaName\":\"tpch\",\"tableName\":\"orders\",\"columnFilters\":{}}";
                PartitionInput o_Input = new PartitionInput();
                o_Input.setTransId(123456);
                ScanTableInfo o_tableInfo = new ScanTableInfo();
                o_tableInfo.setTableName("orders");
                // int o_NumOfFile = (int)Math.ceil((double)tableToInputSplits.get("orders").size()/(double)CFnumber);
                int o_NumOfCF = (int)Math.ceil((double)tableToInputSplits.get("orders").size()/(double)filePerCf);
                System.out.println("total numof cf for orders: " + o_NumOfCF);
                o_tableInfo.setFilter(ordersFilter);
                o_tableInfo.setBase(true);
                o_tableInfo.setColumnsToRead(new String[] { "o_orderkey","o_custkey","o_orderdate","o_totalprice"});
                o_tableInfo.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null, null));
                o_Input.setTableInfo(o_tableInfo);
                o_Input.setProjection(new boolean[] { true, true, true, true });
                PartitionInfo o_partitionInfo = new PartitionInfo();
                o_partitionInfo.setNumPartition(numOfPartition);
                o_partitionInfo.setKeyColumnIds(new int[] {1});
                //**********************************************/
                
                if (enablePartition){
                        InvokerLocal invokerLocal= new InvokerLocal();
                        for(int c_num=0;c_num<c_NumOfCF;c_num++){
                                tableInfo.setInputSplits(tableToInputSplits.get("customer").subList(c_num*filePerCf, (c_num+1)*filePerCf > tableToInputSplits.get("customer").size() ? tableToInputSplits.get("customer").size() : (c_num+1)*filePerCf));
                                input.setPartitionInfo(partitionInfo);
                                input.setOutput(new OutputInfo("jingrong-lambda-test/unit_tests/intermediate_result/customer_partition/Part_" + c_num,
                                new StorageInfo(Storage.Scheme.s3, null, null, null, null), false));
                                
                                
                                if(local){
                                        partitionFuture.add(invokerLocal.invokeLocalPartition(input));
                                }
                                else{
                                        CloudFuture.add(InvokerCloud.invokeCloudPartition(InvokerFactory.Instance(),input));
                                }
                        }

                        for (int o_num=0;o_num<o_NumOfCF;o_num++){
                                o_tableInfo.setInputSplits(tableToInputSplits.get("orders").subList(o_num*filePerCf, (o_num+1)*filePerCf > tableToInputSplits.get("orders").size() ? tableToInputSplits.get("orders").size() : (o_num+1)*filePerCf));
                                o_Input.setPartitionInfo(o_partitionInfo);
                                o_Input.setOutput(new OutputInfo("jingrong-lambda-test/unit_tests/intermediate_result/orders_partition/Part_" + o_num,
                                new StorageInfo(Storage.Scheme.s3, null, null, null, null), false));
                                
                                if(local){
                                        partitionFuture.add(invokerLocal.invokeLocalPartition(o_Input));
                                }
                                else{
                                        CloudFuture.add(InvokerCloud.invokeCloudPartition(InvokerFactory.Instance(),o_Input));
                                }

                        }
                }


                
                /**
                 * scan and partition table info
                 */

                String lineitemFilter = "{\"schemaName\":\"tpch\",\"tableName\":\"lineitem\",\"columnFilters\":{}}";
                JoinScanFusionInput joinScanInput = new JoinScanFusionInput();
                PartitionInput rightPartitionInfo = new PartitionInput();
                rightPartitionInfo.setTransId(123456);
                ScanTableInfo scantableInfo = new ScanTableInfo();
                scantableInfo.setTableName("lineitem");
                // Integer l_NumOfFile = (int)Math.ceil((double)tableToInputSplits.get("lineitem").size()/(double)CFnumber); 

                scantableInfo.setColumnsToRead(new String[] { "l_orderkey", "l_quantity" });
                scantableInfo.setFilter(lineitemFilter);
                scantableInfo.setBase(true);
                scantableInfo.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null, null));
                rightPartitionInfo.setProjection(new boolean[] { true, true });
                PartitionInfo fusionpartitionInfo = new PartitionInfo();
                fusionpartitionInfo.setKeyColumnIds(new int[] { 0 });
                fusionpartitionInfo.setNumPartition(numOfPartition);
                rightPartitionInfo.setPartitionInfo(fusionpartitionInfo);
                
                //set scan pipeline info
                ScanpipeInfo scanpipe = new ScanpipeInfo("lineitem");
                scanpipe.setIncludeCols(new String[] { "l_orderkey", "l_quantity" });
                scanpipe.setProjectFidlds(new String[] { "l_orderkey", "l_quantity" });
                scanpipe.setProjectFieldIds(new int[] { 0, 4 });
                scanpipe.setPartialAggregation(true);
                PartialAggregationInfo parAggregationInfo = new PartialAggregationInfo();
                parAggregationInfo.setGroupKeyColumnNames(new ArrayList<String>(Arrays.asList("l_orderkey")));
                parAggregationInfo.setGroupKeyColumnAlias(new String[] { "l_orderkey" });
                parAggregationInfo.setGroupKeyColumnIds(new int[] { 0 });
                parAggregationInfo.setAggregateColumnIds(new int[] { 1 });
                parAggregationInfo.setResultColumnAlias(new String[] { "SUM_l_quantity" });
                parAggregationInfo.setResultColumnTypes(new String[] { "bigint" });
                parAggregationInfo.setFunctionTypes(new FunctionType[] { FunctionType.SUM });
                parAggregationInfo.setPartition(true);
                parAggregationInfo.setNumPartition(scanpartition);
                scanpipe.setPartialAggregationInfo(parAggregationInfo);
                joinScanInput.setScanPipelineInfo(scanpipe);
                // set fusionOutput
                MultiOutputInfo fusionOutput = new MultiOutputInfo();
                fusionOutput.setPath("s3://jingrong-lambda-test/unit_tests/intermediate_result/");
                fusionOutput.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null, null));
                fusionOutput.setEncoding(false);
                int l_NumOfCF = (int)Math.ceil((double)tableToInputSplits.get("lineitem").size()/(double)filePerCf);
                System.out.println("total size of l_numOfCF: " + l_NumOfCF);
                if(enableScan){
                        for(int l_num=0;l_num<l_NumOfCF;l_num++){
                                // List<InputSplit> lineitemInputSplit = tableToInputSplits.get("lineitem").subList(num, 2);
                                List<InputSplit> lineitemInputSplit = tableToInputSplits.get("lineitem").subList(l_num*filePerCf, (l_num+1)*filePerCf > tableToInputSplits.get("lineitem").size() ? tableToInputSplits.get("lineitem").size() : (l_num+1)*filePerCf);
                                scantableInfo.setInputSplits(lineitemInputSplit);
                                rightPartitionInfo.setTableInfo(scantableInfo);
                                fusionOutput.setFileNames(new ArrayList<String>(
                                                Arrays.asList("partitionoutput1/Part_"+l_num, "lineitem_partition/Part_"+l_num, "scanoutput/Part_"+l_num)));
                                joinScanInput.setFusionOutput(fusionOutput);
                                joinScanInput.setPartitionlargeTable(rightPartitionInfo);
                                

                                if(local){
                                        futures.add(InvokerLocal.invokeLocalFusionJoinScan(joinScanInput));   
                                }
                                else{
                                        CloudFuture.add(InvokerCloud.invokeCloudFusionJoinScan(InvokerFactory.Instance(),joinScanInput));
                                }
                        }
                }


                if (local){
                        if (enablePartition){
                                for(CompletableFuture<PartitionOutput> future: partitionFuture){
                                        try {
                                                PartitionOutput result = future.get();
                                                partitionOutputs.add(result);
                                                System.out.println(JSON.toJSONString(result));
                                        } catch (Exception e) {
                                                e.printStackTrace();
                                        }
                                }
                        }

                        if (enableScan){
                                for(CompletableFuture<FusionOutput> future: futures){
                                        try {
                                                FusionOutput result = future.get();
                                                fusionOutputs.add(result);
                                                System.out.println(JSON.toJSONString(result));
                                        } catch (Exception e) {
                                                e.printStackTrace();
                                        }
                                }
                        }
                }else{  
                        for(CompletableFuture<Output> future: CloudFuture){
                                try {
                                        Output result = future.get();
                                        if (result instanceof PartitionOutput){
                                                partitionOutputs.add((PartitionOutput)result);
                                        }else{
                                                fusionOutputs.add((FusionOutput)result);
                                        }
                                } catch (Exception e) {
                                        e.printStackTrace();
                                }
                        }
                }

                System.out.println("finish the first stage, writing the outputs");
                
                ImmutableTriple<Boolean, List<PartitionOutput>,List<FusionOutput>> result = new ImmutableTriple(true, partitionOutputs,fusionOutputs);
                return result;
        }

        public ImmutableTriple<Boolean, List<JoinOutput>,List<AggregationOutput>> SecondStage(boolean islocal,HashMap<String, List<String>> stage2Files, List<Integer> hashvalue, int HashJoinCFnumber,int scanPartitionNum,int numOfPartition,int numOfPostPartition){

                //cloud future
                List<CompletableFuture<Output>> Cloudfutures = new ArrayList<CompletableFuture<Output>>();
                List<CompletableFuture<JoinOutput>> futures = new ArrayList<CompletableFuture<JoinOutput>>();
                List<CompletableFuture<AggregationOutput>> aggFutures = new ArrayList<CompletableFuture<AggregationOutput>>();

                //outputs
                List<JoinOutput> joinOutput = new ArrayList<JoinOutput>();
                List<AggregationOutput> aggregationOutput = new ArrayList<AggregationOutput>();
                
                // hashjoinCFnumber is the number of CFs used in hashjoin, has to be lower than size of hashvalue
                assertTrue("HashJoinCFnumber is bigger than hashvalue size",HashJoinCFnumber <= hashvalue.size());
                assertTrue("can not be divided", hashvalue.size()%HashJoinCFnumber == 0);

                int cfPerhash = (int)Math.ceil((double)hashvalue.size()/(double)HashJoinCFnumber);

                //partition join customer and orders
                PartitionedJoinInput joinInput = new PartitionedJoinInput();
                joinInput.setTransId(123456);

                PartitionedTableInfo leftTableInfo = new PartitionedTableInfo();
                leftTableInfo.setTableName("customer");
                leftTableInfo.setColumnsToRead(new String[]{"c_custkey", "c_name"});
                leftTableInfo.setKeyColumnIds(new int[]{0});
                leftTableInfo.setInputFiles(stage2Files.get("customer_partition"));
                leftTableInfo.setParallelism(8);
                leftTableInfo.setBase(false);
                leftTableInfo.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null, null));
                joinInput.setSmallTable(leftTableInfo);

                PartitionedTableInfo rightTableInfo = new PartitionedTableInfo();
                rightTableInfo.setTableName("orders");
                rightTableInfo.setColumnsToRead(new String[]{"o_orderkey","o_custkey","o_orderdate","o_totalprice"});
                rightTableInfo.setKeyColumnIds(new int[]{1});
                rightTableInfo.setInputFiles(stage2Files.get("orders_partition"));
                rightTableInfo.setParallelism(8);
                rightTableInfo.setBase(false);
                rightTableInfo.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null, null));
                joinInput.setLargeTable(rightTableInfo);

                PartitionedJoinInfo joinInfo = new PartitionedJoinInfo();
                joinInfo.setJoinType(JoinType.EQUI_INNER);
                joinInfo.setNumPartition(numOfPartition);
               
                // joinInfo.setHashValues(new ArrayList<Integer>(hashValues));
                joinInfo.setSmallColumnAlias(new String[]{"c_custkey", "c_name"});
                joinInfo.setLargeColumnAlias(new String[]{"o_orderkey","o_custkey","o_orderdate","o_totalprice"});
                joinInfo.setSmallProjection(new boolean[]{true, true});
                joinInfo.setLargeProjection(new boolean[]{true, true, true, true});
                joinInfo.setPostPartition(true);
                joinInfo.setPostPartitionInfo(new PartitionInfo(new int[] {2}, numOfPostPartition));

                for (int cf_num=0;cf_num<HashJoinCFnumber;cf_num++){
                        List<Integer> local_hashvalue = new ArrayList<Integer>();
                        for (int hash_num=0;hash_num<cfPerhash;hash_num++){
                                local_hashvalue.add(hashvalue.get(cf_num*cfPerhash+hash_num));
                        }
                        joinInfo.setHashValues(local_hashvalue);
                        joinInput.setJoinInfo(joinInfo);
                        joinInput.setOutput(new MultiOutputInfo("jingrong-lambda-test/unit_tests/intermediate_result/customer_orders_partitionjoin/",
                        new StorageInfo(Storage.Scheme.s3, null, null, null, null),
                        false, Arrays.asList("part_"+cf_num))); // force one file currently
                        
                        if(islocal){
                                futures.add(InvokerLocal.invokeLocalPartitionJoin(joinInput));
                        }
                        else{
                                Cloudfutures.add(InvokerCloud.invokeCloudPartitionJoin(InvokerFactory.Instance(),joinInput));
                        }

                }
                
                //FULL AGGREGATION
                AggregationInput aggregationInput = new AggregationInput();
                aggregationInput.setTransId(123456);
                AggregatedTableInfo aggregatedTableInfo = new AggregatedTableInfo();
                aggregatedTableInfo.setParallelism(8);
                aggregatedTableInfo.setStorageInfo(new StorageInfo(Storage.Scheme.s3,
                null, null, null, null));
                aggregatedTableInfo.setInputFiles(stage2Files.get("scanoutput"));
                // System.out.println(stage2Files.get("scanoutput").subList(0, 8));

                aggregatedTableInfo.setColumnsToRead(new String[] {"l_orderkey", "SUM_l_quantity"});
                aggregatedTableInfo.setBase(false);
                aggregatedTableInfo.setTableName("aggregate_lineitem");
                aggregationInput.setAggregatedTableInfo(aggregatedTableInfo);

                AggregationInfo aggregationInfo = new AggregationInfo();
                aggregationInfo.setGroupKeyColumnIds(new int[] {0});
                aggregationInfo.setAggregateColumnIds(new int[] {1});
                aggregationInfo.setGroupKeyColumnNames(new String[] {"l_orderkey"});
                aggregationInfo.setGroupKeyColumnProjection(new boolean[] {true});
                aggregationInfo.setResultColumnNames(new String[] {"SUM_l_quantity"});
                aggregationInfo.setResultColumnTypes(new String[] {"bigint"});
                aggregationInfo.setFunctionTypes(new FunctionType[] {FunctionType.SUM});
                aggregationInfo.setInputPartitioned(true);
                aggregationInfo.setNumPartition(scanPartitionNum);
                
                //TODO: can do partition after full aggregation 
                
                for(int sc_num=0;sc_num<scanPartitionNum;sc_num++){
                        aggregationInfo.setHashValues(new ArrayList<Integer>(Arrays.asList(sc_num)));
                        aggregationInput.setAggregationInfo(aggregationInfo);
                        aggregationInput.setOutput(new OutputInfo("jingrong-lambda-test/unit_tests/intermediate_result/aggregation_result/part_"+sc_num,
                        new StorageInfo(Storage.Scheme.s3, null, null, null, null), false));
                        if(islocal){
                                aggFutures.add(InvokerLocal.invokeLocalAggregation(aggregationInput));
                        }
                        else{
                                Cloudfutures.add(InvokerCloud.invokeCloudAggregation(InvokerFactory.Instance(),aggregationInput));
                        }
                }
                
                
                if(islocal){
                        for(CompletableFuture<JoinOutput> future: futures){
                                try {
                                        JoinOutput result = future.get();
                                        joinOutput.add(result);
                                        // System.out.println(JSON.toJSONString(result));
                                } catch (Exception e) {
                                        e.printStackTrace();
                                }
                        }

                        for(CompletableFuture<AggregationOutput> future: aggFutures){
                                try {
                                        AggregationOutput result = future.get();
                                        aggregationOutput.add(result);
                                        System.out.println(JSON.toJSONString(result));
                                } catch (Exception e) {
                                        e.printStackTrace();
                                }
                        }
                }else{
                        for(CompletableFuture<Output> future: Cloudfutures){
                                try {
                                        Output result = future.get();
                                        if (result instanceof JoinOutput){
                                                joinOutput.add((JoinOutput)result);
                                                // System.out.println(JSON.toJSONString((JoinOutput)result));
                                        }else{
                                                aggregationOutput.add((AggregationOutput)result);
                                                System.out.println(JSON.toJSONString((AggregationOutput)result));
                                        }

                                } catch (Exception e) {
                                        e.printStackTrace();
                                }
                        }
                }

                System.out.println("stage two finished");

                
                ImmutableTriple<Boolean, List<JoinOutput>,List<AggregationOutput>> result = new ImmutableTriple(true, joinOutput,aggregationOutput);

                return result;
        }

        public ImmutablePair<Boolean, List<JoinOutput>> ThirdStage(boolean islocal,HashMap<String, List<String>> stage3Files, List<Integer> hashvalue, int HashJoinCFnumber,int secondHashJoinCFnumber,int numOfPartition,int numOfPostPartition){
                //cloud future
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
                leftTableInfo.setParallelism(8);
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
                rightTableInfo.setParallelism(2);
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
                for (int cf_num=0;cf_num<HashJoinCFnumber;cf_num++){
                        List<Integer> local_hashvalue = new ArrayList<Integer>();
                        for (int hash_num=0;hash_num<cfPerhash;hash_num++){
                                local_hashvalue.add(hashvalue.get(cf_num*cfPerhash+hash_num));
                        }
                        // System.out.println("hashvalue: " + local_hashvalue);
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



                // now do the partition for aggregation result
                int agg_parNum = 80;

                String leftFilter ="{\"schemaName\":\"tpch\",\"tableName\":\"aggregate_lineitem\"," +
                        "\"columnFilters\":{1:{\"columnName\":\"SUM_l_quantity\",\"columnType\":\"LONG\"," +
                        "\"filterJson\":\"{\\\"javaType\\\":\\\"long\\\",\\\"isAll\\\":false," +
                        "\\\"isNone\\\":false,\\\"allowNull\\\":false,\\\"ranges\\\":[{" +
                        "\\\"lowerBound\\\":{\\\"type\\\":\\\"EXCLUDED\\\",\\\"value\\\":314}," +
                        "\\\"upperBound\\\":{\\\"type\\\":\\\"UNBOUNDED\\\"}}]," +
                        "\\\"discreteValues\\\":[]}\"}}}";
                PartitionInput input = new PartitionInput();
                input.setTransId(123456);
                ScanTableInfo tableInfo = new ScanTableInfo();
                tableInfo.setTableName("aggregation_result");
                tableInfo.setFilter(leftFilter);
                tableInfo.setBase(true);
                tableInfo.setColumnsToRead(new String[] { "l_orderkey", "SUM_l_quantity" });
                tableInfo.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null, null));
                input.setProjection(new boolean[] { true, true });
                PartitionInfo partitionInfo = new PartitionInfo();
                partitionInfo.setNumPartition(agg_parNum);
                partitionInfo.setKeyColumnIds(new int[] {0});
                input.setPartitionInfo(partitionInfo);

                int cfperfile= 15;
                int num_parCf = (int)Math.ceil((double)stage3Files.get("aggregation_result").size()/(double)cfperfile); 

                List<String> aggregation_result_partition = new ArrayList<String>();
                for(int a_pnum=0;a_pnum<num_parCf;a_pnum++){
                        List<String> localString = new ArrayList<String>();
                        List<InputSplit> localinputlist = new ArrayList<InputSplit>();
                        for(int cf_num=0;cf_num<cfperfile;cf_num++){
                                if(a_pnum*cfperfile+cf_num<stage3Files.get("aggregation_result").size()){
                                        localString.add(stage3Files.get("aggregation_result").get(a_pnum*cfperfile+cf_num));
                                }
                        }
                        for(String s:localString){
                                localinputlist.add(new InputSplit(Arrays.asList(new InputInfo(s, 0, -1))));
                        }
                        tableInfo.setInputSplits(localinputlist);
                        input.setTableInfo(tableInfo);
                        input.setOutput(new OutputInfo("jingrong-lambda-test/unit_tests/intermediate_result/aggregation_result_partition/part_"+a_pnum,
                        new StorageInfo(Storage.Scheme.s3, null, null, null, null), false));
                        aggregation_result_partition.add("jingrong-lambda-test/unit_tests/intermediate_result/aggregation_result_partition/part_"+a_pnum);
                        if(islocal){
                                partitionFutures.add(InvokerLocal.invokeLocalPartition(input));
                        }
                        else{
                                Cloudfutures.add(InvokerCloud.invokeCloudPartition(InvokerFactory.Instance(),input));
                        }
                }

                System.out.println("try getting the middle result");
                // only when get the result we can proceed to the next stage
                try {   
                        if(islocal){

                                for(CompletableFuture<PartitionOutput> parfuture: partitionFutures){
                                        try {
                                                PartitionOutput result = parfuture.get();
                                                // partitionOutputs.add(result);
                                                System.out.println(JSON.toJSONString(result));
                                        } catch (Exception e) {
                                                e.printStackTrace();
                                        }
                                }

                                for(CompletableFuture<JoinOutput> future: futures){
                                        try {
                                                JoinOutput result = future.get();
                                                // joinOutput.add(result);
                                                System.out.println(JSON.toJSONString(result));
                                        } catch (Exception e) {
                                                e.printStackTrace();
                                        }
                                }

                        }else  {

                                for(CompletableFuture<Output> future:Cloudfutures){
                                        try{
                                                Output result = future.get();
                                                if (result instanceof JoinOutput){
                                                        // joinOutput.add((JoinOutput)result);
                                                        System.out.println(JSON.toJSONString((JoinOutput)result));
                                                }else if (result instanceof PartitionOutput){
                                                        System.out.println(JSON.toJSONString((PartitionOutput)result));
                                                }
                                        } catch (Exception e) {
                                                e.printStackTrace();
                                        }
                                        
                                }

                        }

                } catch (Exception e) {
                        e.printStackTrace();
                }
                
                //empty future and cloud future
                futures.clear();
                Cloudfutures.clear();


                System.out.println("WE ARE IN THE MIDDLE OF THIRD STAGE");
                // partition join!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                
                PartitionedJoinInput joinInput2 = new PartitionedJoinInput();
                joinInput2.setTransId(123456);
                PartitionedTableInfo leftTableInfo2 = new PartitionedTableInfo();
                leftTableInfo2.setTableName("aggregation_result");
                leftTableInfo2.setColumnsToRead(new String[]{"l_orderkey","SUM_l_quantity"});
                leftTableInfo2.setKeyColumnIds(new int[]{0});
                leftTableInfo2.setParallelism(20);
                leftTableInfo2.setBase(false);
                leftTableInfo2.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null, null));
                leftTableInfo2.setInputFiles(aggregation_result_partition);
                joinInput2.setSmallTable(leftTableInfo2);

                PartitionedTableInfo rightTableInfo2 = new PartitionedTableInfo();
                rightTableInfo2.setTableName("customer_orders_lineitem_partitionjoin");
                rightTableInfo2.setColumnsToRead(new String[]{"c_custkey","c_name","o_orderkey", "o_custkey", "o_orderdate", "o_totalprice", "l_orderkey", "l_quantity"});
                rightTableInfo2.setKeyColumnIds(new int[]{6});
                rightTableInfo2.setBase(false);
                rightTableInfo2.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null, null));
                rightTableInfo2.setParallelism(8);
                rightTableInfo2.setInputFiles(secondStageFiles);
                joinInput2.setLargeTable(rightTableInfo2);

                PartitionedJoinInfo joinInfo2 = new PartitionedJoinInfo();
                joinInfo2.setJoinType(JoinType.EQUI_INNER);
                joinInfo2.setNumPartition(numOfPostPartition);
                joinInfo2.setSmallColumnAlias(new String[]{"l_orderkey","SUM_l_quantity"});
                joinInfo2.setLargeColumnAlias(new String[]{"c_custkey","c_name","o_orderkey", "o_custkey", "o_orderdate", "o_totalprice", "l_orderkey", "l_quantity"});
                joinInfo2.setSmallProjection(new boolean[]{false, false});
                joinInfo2.setLargeProjection(new boolean[]{true, true, true, false, true, true, false, true});
                joinInfo2.setPostPartition(true);
                joinInfo2.setPostPartitionInfo(new PartitionInfo(new int[] {0,1,2,3,4}, 20));
                joinInfo2.setSmallColumnAlias(new String[] { });
                joinInfo2.setLargeColumnAlias(new String[] { "c_custkey","c_name","o_orderkey", "o_orderdate","o_totalprice", "l_quantity" });

                List<Integer> join2Hashvalues = new ArrayList<Integer>();
                for(int i=0;i<numOfPostPartition;i++){
                        join2Hashvalues.add(numOfPostPartition);
                }

                int hash_per_cf= (int)Math.ceil((double)join2Hashvalues.size()/(double)secondHashJoinCFnumber);

                for(int cf_num=0;cf_num<secondHashJoinCFnumber;cf_num++){
                        List<Integer> local_hashvalue = new ArrayList<Integer>();
                        for (int hash_num=0;hash_num<hash_per_cf;hash_num++){
                                local_hashvalue.add(join2Hashvalues.get(cf_num*hash_per_cf+hash_num));
                        }
                        joinInfo2.setHashValues(local_hashvalue);
                        joinInput2.setJoinInfo(joinInfo2);
                        joinInput2.setOutput(new MultiOutputInfo("jingrong-lambda-test/unit_tests/intermediate_result/final_hashjoin_result/",
                        new StorageInfo(Storage.Scheme.s3, null, null, null, null),
                        false, Arrays.asList("part_"+cf_num))); // force one file currently

                        if(islocal){
                                futures.add(InvokerLocal.invokeLocalPartitionJoin(joinInput2));
                        }
                        else{
                                Cloudfutures.add(InvokerCloud.invokeCloudPartitionJoin(InvokerFactory.Instance(),joinInput2));
                        }
                
                }
                ///

                if(islocal){
                        for(CompletableFuture<JoinOutput> future: futures){
                                try {
                                        JoinOutput result = future.get();
                                        joinOutput.add(result);
                                        System.out.println(JSON.toJSONString(result));
                                } catch (Exception e) {
                                        e.printStackTrace();
                                }
                        }
                } else  {
                        for(CompletableFuture<Output> future:Cloudfutures){
                                try{
                                        Output result = future.get();
                                        if (result instanceof JoinOutput){
                                                joinOutput.add((JoinOutput)result);
                                        }
                                } catch (Exception e) {
                                        e.printStackTrace();
                                }
                                
                        }
                }

                System.out.println("WE ARE IN THE END OF THIRD STAGE");

                ImmutablePair<Boolean, List<JoinOutput>> result = new ImmutablePair(true, joinOutput);

                return result;
        }
        
        public boolean FourthStage(boolean islocal,List<String> Stage3inputs,int CFnumber,int numOfPartition){

                //cloud future
                List<CompletableFuture<Output>> Cloudfutures = new ArrayList<CompletableFuture<Output>>();
                List<CompletableFuture<AggregationOutput>> futures = new ArrayList<CompletableFuture<AggregationOutput>>();

                //Groupby result
                AggregationInput aggregationInput = new AggregationInput();
                aggregationInput.setTransId(123456);
                AggregatedTableInfo aggregatedTableInfo = new AggregatedTableInfo();
                aggregatedTableInfo.setParallelism(8);
                aggregatedTableInfo.setStorageInfo(new StorageInfo(Storage.Scheme.s3,
                null, null, null, null));
                aggregatedTableInfo.setInputFiles(Stage3inputs);
                aggregatedTableInfo.setColumnsToRead(new String[] {"c_name","c_custkey","o_orderkey", "o_orderdate","o_totalprice", "l_quantity"});
                aggregatedTableInfo.setBase(true);
                aggregatedTableInfo.setTableName("final_result");
                aggregationInput.setAggregatedTableInfo(aggregatedTableInfo);
                AggregationInfo aggregationInfo = new AggregationInfo();
                aggregationInfo.setGroupKeyColumnIds(new int[] {0,1,2,3,4});
                aggregationInfo.setAggregateColumnIds(new int[] {5});
                aggregationInfo.setGroupKeyColumnNames(new String[] {"c_name","c_custkey","o_orderkey", "o_orderdate","o_totalprice"});
                aggregationInfo.setGroupKeyColumnProjection(new boolean[] {true, true, true, true, true});
                aggregationInfo.setResultColumnNames(new String[] {"sum_l_quantity"});
                aggregationInfo.setResultColumnTypes(new String[] {"bigint"});
                aggregationInfo.setFunctionTypes(new FunctionType[] {FunctionType.SUM});
                aggregationInput.setAggregationInfo(aggregationInfo);
                aggregationInput.setOutput(new OutputInfo("jingrong-lambda-test/unit_tests/intermediate_result/final_final_result",
                new StorageInfo(Storage.Scheme.s3, null, null, null, null), true));


                int hash_per_cf = (int)Math.ceil((double)numOfPartition/(double) CFnumber);
                
                for(int sc_num=0;sc_num<CFnumber;sc_num++){
                        List<Integer> local_hash = new ArrayList<Integer>();
                        for (int hash_num=0;hash_num<hash_per_cf;hash_num++){
                                local_hash.add(sc_num*hash_per_cf+hash_num);
                        }

                        aggregationInfo.setHashValues(local_hash);
                        aggregationInput.setAggregationInfo(aggregationInfo);
                        aggregationInput.setOutput(new OutputInfo("jingrong-lambda-test/unit_tests/intermediate_result/final_final_result/part_"+sc_num,
                        new StorageInfo(Storage.Scheme.s3, null, null, null, null), false));

                        if(islocal){
                                futures.add(InvokerLocal.invokeLocalAggregation(aggregationInput));
                        }
                        else{
                                Cloudfutures.add(InvokerCloud.invokeCloudAggregation(InvokerFactory.Instance(),aggregationInput));
                        }
                        
                }

                if(islocal){
                        for(CompletableFuture<AggregationOutput> future: futures){
                                try {
                                        AggregationOutput result = future.get();
                                        System.out.println(JSON.toJSONString(result));
                                } catch (Exception e) {
                                        e.printStackTrace();
                                }
                        }
                }else{
                        for(CompletableFuture<Output> future:Cloudfutures){
                                try{
                                        Output result = future.get();
                                        if (result instanceof AggregationOutput){
                                                System.out.println(JSON.toJSONString((AggregationOutput)result));
                                        }
                                } catch (Exception e) {
                                        e.printStackTrace();
                                }
                                
                        }
                }


                return true;
        }

        @Test
        public void TestTPCHQ18() {
                System.setProperty("PIXELS_HOME", "/home/ubuntu/opt/pixels");
                String pixelsHome = System.getenv("PIXELS_HOME");
                
                // Chose local or cloud
                boolean test=false;
                boolean local = false;
                int numOfPartition = 40;
                int numOfPostPartition = 80;
                int numOfScanPartition = 40;
                //prepraing the input
                HashMap<String, List<InputSplit>> tableToInputSplits = Common.getTableToInputSplits(Arrays.asList("customer","lineitem","orders"));
                long startTime = System.nanoTime();

                //stage 1 running

                //numof partition is for customer and orders
                //numof post partition is for lineitem
                ImmutableTriple<Boolean, List<PartitionOutput>,List<FusionOutput>> stageOneOUT = FirstStage(local,true,true,tableToInputSplits,15,numOfScanPartition,numOfPartition);
                boolean boolstageOne=stageOneOUT.getLeft();

                /*
                 * get output from stage 1
                 */
                HashMap<String, List<String>> stage2Files = new HashMap<String, List<String>>();
                List<String> customer_partition = new ArrayList<String>();
                List<String> orders_partition = new ArrayList<String>();
                List<String> lineitem_partition = new ArrayList<String>();
                List<String> scanoutput = new ArrayList<String>();
                for(PartitionOutput out:stageOneOUT.getMiddle()){
                        if(out.getPath().contains("customer_partition")){
                                customer_partition.add(out.getPath());
                                // System.out.println("customer_partition path: "+out.getPath());
                        }else if(out.getPath().contains("orders_partition")){
                                orders_partition.add(out.getPath());
                                // System.out.println("orders_partition path: "+out.getPath());
                        }
                }

                for(FusionOutput out:stageOneOUT.getRight()){
                        scanoutput.addAll(out.getOutputs());

                        if(out.getSecondPartitionOutput()==null){
                                System.out.println("secondPartitionOutput is null");
                        }else{
                                lineitem_partition.add(out.getSecondPartitionOutput().getPath());
                        }
                }    
                
                stage2Files.put("customer_partition", customer_partition);
                stage2Files.put("orders_partition", orders_partition);
                stage2Files.put("lineitem_partition", lineitem_partition);
                stage2Files.put("scanoutput", scanoutput);
                /********************************************** */
                //for stage two testing

                // HashMap<String, List<String>> testFiles = new HashMap<String, List<String>>();
                // stage2Files = getTableToString("s3://jingrong-lambda-test/unit_tests/intermediate_result/",Arrays.asList("customer_partition","orders_partition","lineitem_partition","scanoutput"));
                
                // System.out.println("tets stage2Files");
                // for(String test: stage2Files.get("scanoutput")){
                //         System.out.println("scanoutput: "+test);
                // }
                // boolean boolstageOne = true;


                boolean boolstageTwo = false;
                List<String> customer_orders_partitionjoin = new ArrayList<String>();
                List<String> aggregation_result = new ArrayList<String>();
                if(boolstageOne){
                        System.out.println("Stage one success!");
                        
                        // Both existing hash values
                        // Set<Integer> left = stageOneOUT.getMiddle().get(0).getHashValues();
                        // Set<Integer> right = stageOneOUT.getMiddle().get(1).getHashValues();

                        // System.out.println("print left");
                        // left.forEach(System.out::println);
                        // System.out.println("print left");
                        // right.forEach(System.out::println);

                        // List<Integer> hashValues = new ArrayList<Integer>();
                        // for (Integer i : left) {
                        //         if (right.contains(i)) {
                        //                 hashValues.add(i);
                        //         }
                        // }
                        // System.out.println("hashValues: " + hashValues);
                        List<Integer> testHashvalues = new ArrayList<Integer>(numOfPartition);
                        for(int i=0;i<numOfPartition;i++){
                                testHashvalues.add(i);
                        }

                        // stage always has 1 full aggregation
                        ImmutableTriple<Boolean, List<JoinOutput>,List<AggregationOutput>> stageTwo = SecondStage(local,stage2Files, testHashvalues,numOfPartition,40,numOfPartition,numOfPostPartition);
                        for(JoinOutput out:stageTwo.getMiddle()){
                                customer_orders_partitionjoin.addAll(out.getOutputs());
                        }
                        for(AggregationOutput out:stageTwo.getRight()){
                                aggregation_result.addAll(out.getOutputs());
                        }

                        // Set<Integer> stage2left = stageTwo.getMiddle().get(0).getHashValues();
                        // Set<Integer> stage2right = stageOneOUT.getRight().get(0).
                        // List<Integer> hashValues = new ArrayList<Integer>();
                        // for (Integer i : left) {
                        //         if (right.contains(i)) {
                        //                 hashValues.add(i);
                        //         }
                        // }
                        // System.out.println("hashValues: " + hashValues);


                        boolstageTwo = stageTwo.getLeft();
                }
                else{
                        System.out.println("Stage one failed!");
                        return;
                }
                


                HashMap<String, List<String>> stage3Files = new HashMap<String, List<String>>();
                stage3Files.put("lineitem_partition", lineitem_partition);
                stage3Files.put("customer_orders_partitionjoin", customer_orders_partitionjoin);
                stage3Files.put("aggregation_result", aggregation_result);
                // boolean boolstageTwo = true;
                // HashMap<String, List<String>> stage3Files = new HashMap<String, List<String>>();
                // stage3Files = Common.getTableToString("s3://jingrong-lambda-test/unit_tests/intermediate_result/",Arrays.asList("lineitem_partition","customer_orders_partitionjoin","aggregation_result"));
                // long startTime = System.nanoTime();
                boolean boolstageThree = false;
                List<String> Stage4inputs = new ArrayList<String>();
                System.out.println("entering stage three");
                if(boolstageTwo){
                        System.out.println("Stage two success!!!!");
                        List<Integer> testHashvalues = new ArrayList<Integer>(numOfPostPartition);
                        for(int i=0;i<numOfPostPartition;i++){
                                testHashvalues.add(i);
                        }

                        ImmutablePair<Boolean, List<JoinOutput>> stageThree = ThirdStage(local,stage3Files, testHashvalues, 40,40,80,numOfPostPartition);
                        for(JoinOutput out:stageThree.getRight()){
                                Stage4inputs.addAll(out.getOutputs());
                        }
                        boolstageThree = stageThree.getLeft();
                } else{
                        System.out.println("Stage two failed!");
                        return;
                }


                

                Boolean boolstageFour = false;
                if(boolstageThree){
                        System.out.println("Stage three success!!!!");
                        boolstageFour = FourthStage(local,Stage4inputs,20,20);
                } else{
                        System.out.println("Stage three failed!");
                        return;
                }

                if(boolstageFour){
                        System.out.println("Stage four success!!!!");
                } else{
                        System.out.println("Stage four failed!");
                        return;
                }

                System.out.println("All stages success!!!!");
                
                long endTime   = System.nanoTime();
                System.out.println("Cloud function Q18 runtime: " + (endTime - startTime)/1000000 + "ms");


               

        }

       
        
                 // String leftFilter ="{\"schemaName\":\"tpch\",\"tableName\":\"aggregate_lineitem\"," +
                //         "\"columnFilters\":{1:{\"columnName\":\"SUM_l_quantity\",\"columnType\":\"LONG\"," +
                //         "\"filterJson\":\"{\\\"javaType\\\":\\\"long\\\",\\\"isAll\\\":false," +
                //         "\\\"isNone\\\":false,\\\"allowNull\\\":false,\\\"ranges\\\":[{" +
                //         "\\\"lowerBound\\\":{\\\"type\\\":\\\"EXCLUDED\\\",\\\"value\\\":314}," +
                //         "\\\"upperBound\\\":{\\\"type\\\":\\\"UNBOUNDED\\\"}}]," +
                //         "\\\"discreteValues\\\":[]}\"}}}";

                
                // //right filter need to be changed
                // String rightFilter = "{\"schemaName\":\"tpch\",\"tableName\":\"partitioned_join_customer_orders_lineitem\",\"columnFilters\":{}}";

                // BroadcastJoinInput brodcastjoinInput = new BroadcastJoinInput();
                // brodcastjoinInput.setTransId(123456);
                // BroadcastTableInfo leftTable = new BroadcastTableInfo();
                // leftTable.setColumnsToRead(new String[] { "l_orderkey", "SUM_l_quantity" });
                // leftTable.setKeyColumnIds(new int[]{0});
                // leftTable.setTableName("aggregate_lineitem");
                // leftTable.setBase(true);

                // List<InputSplit> leftInputSplit = new ArrayList<InputSplit>();
                // for(String file: stage3Files.get("aggregation_result")){
                //         InputSplit temp = new InputSplit(Arrays.asList(new InputInfo(file, 0, -1)));
                //         leftInputSplit.add(temp);
                // }

                // leftTable.setInputSplits(leftInputSplit);
                // leftTable.setFilter(leftFilter);
                // leftTable.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null, null));
                // brodcastjoinInput.setSmallTable(leftTable);

                // BroadcastTableInfo rightTable = new BroadcastTableInfo();
                // rightTable.setColumnsToRead(new String[] { "c_custkey","c_name","o_orderkey", "o_custkey", "o_orderdate","o_totalprice", "l_orderkey", "l_quantity" });
                // rightTable.setKeyColumnIds(new int[]{6});
                // rightTable.setTableName("partitioned_join_customer_orders_lineitem");
                // rightTable.setBase(true);
                // rightTable.setFilter(rightFilter);
                // rightTable.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null, null));
                

                // JoinInfo joinInfo2 = new JoinInfo();
                // joinInfo2.setJoinType(JoinType.EQUI_INNER);
                // joinInfo2.setSmallColumnAlias(new String[] { });
                // joinInfo2.setLargeColumnAlias(new String[] { "c_custkey","c_name","o_orderkey", "o_orderdate","o_totalprice", "l_quantity" });
                // joinInfo2.setPostPartition(false);
                // joinInfo2.setSmallProjection(new boolean[] { false, false });
                // joinInfo2.setLargeProjection(new boolean[] { true, true, true, false, true, true, false, true });

                // int fileperBr= (int)Math.ceil((double)HashJoinCFnumber/(double)brocastJoinCFnumber);
                // assertTrue("fileperbr has to be divided", HashJoinCFnumber%brocastJoinCFnumber == 0);
                // for(int br_num=0;br_num<brocastJoinCFnumber ;br_num++){
                //         List<String> local_secondStageFiles = new ArrayList<String>();
                //         for(int file_num=0;file_num<fileperBr;file_num++){
                //                 local_secondStageFiles.add(secondStageFiles.get(br_num*fileperBr+file_num));
                //         }
                //         List<InputSplit> rightInputSplit = new ArrayList<InputSplit>();
                //         for(String file: local_secondStageFiles){
                //                 InputSplit temp = new InputSplit(Arrays.asList(new InputInfo(file, 0, -1)));
                //                 rightInputSplit.add(temp);
                //         }
                //         // rightInputSplit.forEach(System.out::println);
                        

                //         rightTable.setInputSplits(rightInputSplit);
                //         brodcastjoinInput.setLargeTable(rightTable);
                //         brodcastjoinInput.setJoinInfo(joinInfo2);
                //         brodcastjoinInput.setOutput(new MultiOutputInfo("jingrong-lambda-test/unit_tests/intermediate_result/broadcast_join/",
                //         new StorageInfo(Storage.Scheme.s3, null, null, null, null), false,
                //         Arrays.asList("part_"+br_num)));

                //         if(islocal){
                //                 futures.add(InvokerLocal.invokeLocalBrocastJoin(brodcastjoinInput));
                //         }
                //         else{
                //                 Cloudfutures.add(InvokerCloud.invokeCloudBrocastJoin(InvokerFactory.Instance(),brodcastjoinInput));
                //         }
                // }


}



