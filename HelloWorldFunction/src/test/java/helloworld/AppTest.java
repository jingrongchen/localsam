package helloworld;

import org.junit.Test;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.turbo.InvokerFactory;
import io.pixelsdb.pixels.common.turbo.Output;
import io.pixelsdb.pixels.common.turbo.WorkerType;
import io.pixelsdb.pixels.planner.plan.physical.domain.*;
import io.pixelsdb.pixels.planner.plan.physical.input.ThreadScanInput;
import io.pixelsdb.pixels.planner.plan.physical.input.ScanInput;
import java.util.Arrays;
import java.util.ArrayList;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.lang.Math;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.CloudFrontEvent.CF;

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
import io.pixelsdb.pixels.planner.plan.physical.output.FusionOutput;

import io.pixelsdb.pixels.planner.plan.physical.input.JoinScanFusionInput;
import io.pixelsdb.pixels.planner.plan.physical.input.PartitionInput;
import io.pixelsdb.pixels.executor.join.JoinType;
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
public class AppTest {

        @Test
        public void testGetCF() {
                int memorySize = InvokerFactory.Instance().getInvoker(WorkerType.THREAD_SCAN).getMemoryMB();
                System.out.println(memorySize);

        }

        public CompletableFuture<FusionOutput> invokeLocalFusionJoinScan(JoinScanFusionInput joinScanInput){
                WorkerMetrics workerMetrics = new WorkerMetrics();
                Logger logger = LogManager.getLogger(AppTest.class);
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

        public CompletableFuture<JoinOutput> invokeLocalPartitionJoin(PartitionedJoinInput joinInput) {
                        WorkerMetrics workerMetrics = new WorkerMetrics();
                        Logger logger = LogManager.getLogger(AppTest.class);
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

        public CompletableFuture<Output> invokeLocalPartition(PartitionInput partitionInput) {
                        WorkerMetrics workerMetrics = new WorkerMetrics();
                        Logger logger = LogManager.getLogger(AppTest.class);
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

        public CompletableFuture<AggregationOutput> invokeLocalAggregation(AggregationInput aggregationInput){
                WorkerMetrics workerMetrics = new WorkerMetrics();
                Logger logger = LogManager.getLogger(AppTest.class);
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

        public boolean FirstStage(HashMap<String, List<InputSplit>> tableToInputSplits,int CFnumber) {
                List<CompletableFuture<Output>> partitionFuture = new ArrayList<CompletableFuture<Output>>();
                List<CompletableFuture<FusionOutput>> futures = new ArrayList<CompletableFuture<FusionOutput>>();
                
                for (int p_num=0;p_num<1;p_num++){
                        // partition customer
                        String customerFilter = "{\"schemaName\":\"tpch\",\"tableName\":\"customer\",\"columnFilters\":{}}";
                        PartitionInput input = new PartitionInput();
                        input.setTransId(123456);
                        ScanTableInfo tableInfo = new ScanTableInfo();
                        tableInfo.setTableName("customer");
                        int c_NumOfFile = (int)Math.ceil((double)tableToInputSplits.get("customer").size()/(double)CFnumber);
                        // tableInfo.setInputSplits(tableToInputSplits.get("customer").subList(p_num*c_NumOfFile, (p_num+1)*c_NumOfFile > tableToInputSplits.get("customer").size() ? tableToInputSplits.get("customer").size() : (p_num+1)*c_NumOfFile));
                        tableInfo.setInputSplits(tableToInputSplits.get("customer").subList(p_num*c_NumOfFile, 1));
                        tableInfo.setFilter(customerFilter);
                        tableInfo.setBase(true);
                        tableInfo.setColumnsToRead(new String[] { "c_custkey","c_name"});
                        tableInfo.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null, null));
                        input.setTableInfo(tableInfo);
                        input.setProjection(new boolean[] { true, true });
                        PartitionInfo partitionInfo = new PartitionInfo();
                        partitionInfo.setNumPartition(20);
                        partitionInfo.setKeyColumnIds(new int[] {0});
                        input.setPartitionInfo(partitionInfo);
                        input.setOutput(new OutputInfo("jingrong-lambda-test/unit_tests/intermediate_result/customer_partition/Part_" + p_num,
                        new StorageInfo(Storage.Scheme.s3, null, null, null, null), true));
                        partitionFuture.add(invokeLocalPartition(input));

                        // partition orders
                        // String ordersFilter = "{\"schemaName\":\"tpch\",\"tableName\":\"orders\",\"columnFilters\":{}}";
                        // PartitionInput o_Input = new PartitionInput();
                        // o_Input.setTransId(123456);
                        // ScanTableInfo o_tableInfo = new ScanTableInfo();
                        // o_tableInfo.setTableName("orders");
                        // int o_NumOfFile = (int)Math.ceil((double)tableToInputSplits.get("orders").size()/(double)CFnumber);
                        // o_tableInfo.setInputSplits(tableToInputSplits.get("orders").subList(p_num*o_NumOfFile, (p_num+1)*o_NumOfFile > tableToInputSplits.get("orders").size() ? tableToInputSplits.get("orders").size() : (p_num+1)*o_NumOfFile));
                        // o_tableInfo.setFilter(ordersFilter);
                        // o_tableInfo.setBase(true);
                        // o_tableInfo.setColumnsToRead(new String[] { "o_orderkey","o_custkey","o_orderdate","o_totalprice"});
                        // o_tableInfo.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null, null));
                        // o_Input.setTableInfo(o_tableInfo);
                        // o_Input.setProjection(new boolean[] { true, true, true, true });
                        // PartitionInfo o_partitionInfo = new PartitionInfo();
                        // o_partitionInfo.setNumPartition(20);
                        // o_partitionInfo.setKeyColumnIds(new int[] {1});
                        // o_Input.setPartitionInfo(o_partitionInfo);
                        // o_Input.setOutput(new OutputInfo("jingrong-lambda-test/unit_tests/intermediate_result/orders_partition/Part_" + p_num,
                        // new StorageInfo(Storage.Scheme.s3, null, null, null, null), true));
                        // partitionFuture.add(invokeLocalPartition(o_Input));
                }

                
                // 20 files are maximum for partition
                // for(int num=0;num<1;num++){
                //         String lineitemFilter = "{\"schemaName\":\"tpch\",\"tableName\":\"lineitem\",\"columnFilters\":{}}";
                //         JoinScanFusionInput joinScanInput = new JoinScanFusionInput();

                //         PartitionInput rightPartitionInfo = new PartitionInput();
                //         rightPartitionInfo.setTransId(123456);
                //         ScanTableInfo tableInfo = new ScanTableInfo();
                //         tableInfo.setTableName("lineitem");
                //         Integer l_NumOfFile = (int)Math.ceil((double)tableToInputSplits.get("lineitem").size()/(double)CFnumber);
                //         List<InputSplit> lineitemInputSplit = tableToInputSplits.get("lineitem").subList(num*l_NumOfFile,20);
                //         // List<InputSplit> lineitemInputSplit = tableToInputSplits.get("lineitem").subList(num*l_NumOfFile, (num+1)*l_NumOfFile > tableToInputSplits.get("lineitem").size() ? tableToInputSplits.get("lineitem").size() : (num+1)*l_NumOfFile);
                //         tableInfo.setInputSplits(lineitemInputSplit);
                //         tableInfo.setColumnsToRead(new String[] { "l_orderkey", "l_quantity" });
                //         tableInfo.setFilter(lineitemFilter);
                //         tableInfo.setBase(true);
                //         tableInfo.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null, null));
                //         rightPartitionInfo.setTableInfo(tableInfo);
                //         rightPartitionInfo.setProjection(new boolean[] { true, true });
                //         PartitionInfo partitionInfo = new PartitionInfo();
                //         partitionInfo.setKeyColumnIds(new int[] { 0 });
                //         partitionInfo.setNumPartition(20);
                //         rightPartitionInfo.setPartitionInfo(partitionInfo);

                //         joinScanInput.setPartitionlargeTable(rightPartitionInfo);
                //         ScanPipeInfo scanPipeInfo = new ScanPipeInfo();
                //         scanPipeInfo.setIncludeCols(new String[] { "l_orderkey", "l_quantity" });
                //         scanPipeInfo.setRootTableName("lineitem");
                //         // 1.project
                //         LogicalProject logicalProject = new LogicalProject(new String[] { "l_orderkey", "l_quantity" },
                //                         new int[] { 0, 4 });
                //         scanPipeInfo.addOperation(logicalProject);
                //         // 2.aggregate

                //         LogicalAggregate logicalAggregate = new LogicalAggregate("SUM", "DECIMAL", new int[] { 0 },
                //                         new int[] { 1 });
                //         logicalAggregate.setGroupKeyColumnNames(new String[] { "l_orderkey" });
                //         logicalAggregate.setGroupKeyColumnAlias(new String[] { "l_orderkey" });
                //         logicalAggregate.setResultColumnAlias(new String[] { "SUM_l_quantity" });
                //         logicalAggregate.setResultColumnTypes(new String[] { "DECIMAL" });
                //         logicalAggregate.setPartition(false);
                //         logicalAggregate.setNumPartition(0);
                //         logicalAggregate.setFunctionTypes(new FunctionType[] { FunctionType.SUM });

                //         scanPipeInfo.addOperation(logicalAggregate);
                //         joinScanInput.setScanPipelineInfo(scanPipeInfo);

                //         // set fusionOutput
                //         MultiOutputInfo fusionOutput = new MultiOutputInfo();
                //         fusionOutput.setPath("s3://jingrong-lambda-test/unit_tests/intermediate_result/");
                //         fusionOutput.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null, null));
                //         fusionOutput.setEncoding(false);
                //         fusionOutput.setFileNames(new ArrayList<String>(
                //                         Arrays.asList("partitionoutput1/Part_"+num, "partitionoutput2/Part_"+num, "scanoutput/Part_"+num)));
                //         joinScanInput.setFusionOutput(fusionOutput);

                //         // futures.add(invokeLocalFusionJoinScan(joinScanInput));             
                // }

                for(CompletableFuture<Output> future: partitionFuture){
                        try {
                                Output result = future.get();
                                System.out.println(JSON.toJSONString(result));
                        } catch (Exception e) {
                                e.printStackTrace();
                        }
                }

                // for(CompletableFuture<FusionOutput> future: futures){
                //         try {
                //                 FusionOutput result = future.get();
                //                 System.out.println(JSON.toJSONString(result));
                //         } catch (Exception e) {
                //                 e.printStackTrace();
                //         }
                // }

                return true;
        }

        public boolean SecondStage(HashMap<String, List<String>> stage2Files,int CFnumber){
                //TODO: FINISH SECOND STAGE
                List<CompletableFuture<JoinOutput>> futures = new ArrayList<CompletableFuture<JoinOutput>>();
                List<CompletableFuture<AggregationOutput>> aggFutures = new ArrayList<CompletableFuture<AggregationOutput>>();
                //partition join customer and orders
                Set<Integer> hashValues = new HashSet<>(20);
                for (int i = 0 ; i < 20; ++i)
                {
                        hashValues.add(i);
                }
                PartitionedJoinInput joinInput = new PartitionedJoinInput();
                joinInput.setTransId(123456);
                PartitionedTableInfo leftTableInfo = new PartitionedTableInfo();
                leftTableInfo.setTableName("customer");
                leftTableInfo.setColumnsToRead(new String[]{"c_custkey", "c_name"});
                leftTableInfo.setKeyColumnIds(new int[]{0});
                leftTableInfo.setInputFiles(stage2Files.get("customer_partition"));
                leftTableInfo.setParallelism(20);
                leftTableInfo.setBase(false);
                leftTableInfo.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null, null));
                joinInput.setSmallTable(leftTableInfo);

                PartitionedTableInfo rightTableInfo = new PartitionedTableInfo();
                rightTableInfo.setTableName("orders");
                rightTableInfo.setColumnsToRead(new String[]{"o_orderkey","o_custkey","o_orderdate","o_totalprice"});
                rightTableInfo.setKeyColumnIds(new int[]{1});
                rightTableInfo.setInputFiles(stage2Files.get("orders_partition"));
                rightTableInfo.setParallelism(20);
                rightTableInfo.setBase(false);
                rightTableInfo.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null, null));
                joinInput.setLargeTable(rightTableInfo);

                PartitionedJoinInfo joinInfo = new PartitionedJoinInfo();
                joinInfo.setJoinType(JoinType.EQUI_INNER);
                joinInfo.setNumPartition(20);
                joinInfo.setHashValues(new ArrayList<Integer>(hashValues));
                joinInfo.setSmallColumnAlias(new String[]{"c_custkey", "c_name"});
                joinInfo.setLargeColumnAlias(new String[]{"o_orderkey","o_custkey","o_orderdate","o_totalprice"});
                joinInfo.setSmallProjection(new boolean[]{true, true});
                joinInfo.setLargeProjection(new boolean[]{true, true, true, true});

                joinInfo.setPostPartition(true);
                joinInfo.setPostPartitionInfo(new PartitionInfo(new int[] {2}, 20));
                joinInput.setJoinInfo(joinInfo);

                joinInput.setOutput(new MultiOutputInfo("pixels-lambda-test/unit_tests/intermediate_result/",
                new StorageInfo(Storage.Scheme.s3, null, null, null, null),
                true, Arrays.asList("partitioned_join_customer_orders_0"))); // force one file currently
                futures.add(invokeLocalPartitionJoin(joinInput));


                //FULL AGGREGATION
                AggregationInput aggregationInput = new AggregationInput();
                aggregationInput.setTransId(123456);
                AggregatedTableInfo aggregatedTableInfo = new AggregatedTableInfo();
                aggregatedTableInfo.setParallelism(8);
                aggregatedTableInfo.setStorageInfo(new StorageInfo(Storage.Scheme.s3,
                null, null, null, null));
                aggregatedTableInfo.setInputFiles(stage2Files.get("scanoutput"));
                aggregatedTableInfo.setColumnsToRead(new String[] {"l_orderkey", "l_quantity"});
                aggregatedTableInfo.setBase(false);
                aggregatedTableInfo.setTableName("aggregate_lineitem");
                aggregationInput.setAggregatedTableInfo(aggregatedTableInfo);

                AggregationInfo aggregationInfo = new AggregationInfo();
                aggregationInfo.setGroupKeyColumnIds(new int[] {0});
                aggregationInfo.setAggregateColumnIds(new int[] {1});
                aggregationInfo.setGroupKeyColumnNames(new String[] {"l_orderkey"});
                aggregationInfo.setGroupKeyColumnProjection(new boolean[] {true});
                aggregationInfo.setResultColumnNames(new String[] {"sum_l_quantity"});
                aggregationInfo.setResultColumnTypes(new String[] {"bigint"});
                aggregationInfo.setFunctionTypes(new FunctionType[] {FunctionType.SUM});
                aggregationInput.setAggregationInfo(aggregationInfo);
                
                aggregationInput.setOutput(new OutputInfo("pixels-lambda-test/unit_tests/intermediate_result/aggregation_result",
                new StorageInfo(Storage.Scheme.s3, null, null, null, null), true));
                aggFutures.add(invokeLocalAggregation(aggregationInput));



                for(CompletableFuture<JoinOutput> future: futures){
                        try {
                                JoinOutput result = future.get();
                                System.out.println(JSON.toJSONString(result));
                        } catch (Exception e) {
                                e.printStackTrace();
                        }
                }

                for(CompletableFuture<AggregationOutput> future: aggFutures){
                        try {
                                AggregationOutput result = future.get();
                                System.out.println(JSON.toJSONString(result));
                        } catch (Exception e) {
                                e.printStackTrace();
                        }
                }


                return true;
        }

        public boolean ThirdStage(HashMap<String, List<String>> stage3Files,int CFnumber){
                // customer and orders join result join lineitem partitions
                
                List<CompletableFuture<JoinOutput>> futures = new ArrayList<CompletableFuture<JoinOutput>>();
                List<CompletableFuture<AggregationOutput>> aggFutures = new ArrayList<CompletableFuture<AggregationOutput>>();

                Set<Integer> hashValues = new HashSet<>(20);
                for (int i = 0 ; i < 20; ++i)
                {
                    hashValues.add(i);
                }
                PartitionedJoinInput joinInput = new PartitionedJoinInput();
                joinInput.setTransId(123456);
                PartitionedTableInfo leftTableInfo = new PartitionedTableInfo();
                leftTableInfo.setTableName("customer_orders");
                leftTableInfo.setColumnsToRead(new String[]{"c_custkey","c_name","o_orderkey", "o_custkey", "o_orderstatus", "o_orderdate"});
                leftTableInfo.setKeyColumnIds(new int[]{2});
                leftTableInfo.setInputFiles(stage3Files.get("customer_orders_partition"));
                leftTableInfo.setParallelism(8);
                leftTableInfo.setBase(false);
                leftTableInfo.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null, null));
                joinInput.setSmallTable(leftTableInfo);
                //filter the result of aggregation

                PartitionedTableInfo rightTableInfo = new PartitionedTableInfo();
                rightTableInfo.setTableName("partition_lineitem");
                rightTableInfo.setColumnsToRead(new String[]{"l_orderkey", "sum_l_quantity"});
                rightTableInfo.setKeyColumnIds(new int[]{0});
                rightTableInfo.setInputFiles(stage3Files.get("partition_lineitem"));
                rightTableInfo.setParallelism(2);
                rightTableInfo.setBase(false);
                rightTableInfo.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null, null));
                joinInput.setLargeTable(rightTableInfo);

                PartitionedJoinInfo joinInfo = new PartitionedJoinInfo();
                joinInfo.setJoinType(JoinType.EQUI_INNER);
                joinInfo.setNumPartition(20);
                joinInfo.setHashValues(new ArrayList<Integer>(hashValues));
                joinInfo.setSmallColumnAlias(new String[]{"c_custkey","c_name","o_orderkey", "o_custkey", "o_orderstatus", "o_orderdate"});
                joinInfo.setLargeColumnAlias(new String[]{"l_orderkey", "l_quantity"});
                joinInfo.setSmallProjection(new boolean[]{true, true, true, true, true, true});
                joinInfo.setLargeProjection(new boolean[]{false, true});
                joinInfo.setPostPartition(false);
                joinInput.setJoinInfo(joinInfo);

                joinInput.setOutput(new MultiOutputInfo("pixels-lambda-test/unit_tests/intermediate_result/",
                new StorageInfo(Storage.Scheme.s3, null, null, null, null),
                true, Arrays.asList("partitioned_join_customer_orders_lineitem_0"))); // force one file currently
                
                try {
                        JoinOutput result = invokeLocalPartitionJoin(joinInput).get();
                        System.out.println(JSON.toJSONString(result));
                } catch (Exception e) {
                        e.printStackTrace();
                }
                //once get the result start brocast join

                String leftFilter = "{\"schemaName\":\"tpch\",\"tableName\":\"lineitem\",\"columnFilters\":{}}";
                
                //right filter need to be changed
                String rightFilter = "{\"schemaName\":\"tpch\",\"tableName\":\"lineitem\",\"columnFilters\":{}}";

                BroadcastJoinInput brodcastjoinInput = new BroadcastJoinInput();
                brodcastjoinInput.setTransId(123456);

                BroadcastTableInfo leftTable = new BroadcastTableInfo();
                leftTable.setColumnsToRead(new String[] { "l_orderkey", "l_quantity" });



                


                return true;
        }
        @Test
        public void TestTPCHQ18() {
                HashMap<String, List<InputSplit>> tableToInputSplits = new HashMap<String, List<InputSplit>>();
                // "orders", "lineitem"
                for (String tableName : Arrays.asList("customer")) {
                        try {
                                List<InputSplit> paths = new ArrayList<InputSplit>();
                                List<String> filePath = new ArrayList<String>();

                                String storagepath = "s3://jingrong-lambda-test/tpch/" + tableName + "/" + "v-0-ordered" + "/";
                                System.out.println("storagepath: " + storagepath);
                                Storage storage = StorageFactory.Instance().getStorage(storagepath);
                                filePath = storage.listPaths(storagepath);

                                for (String line : filePath) {
                                        InputSplit temp = new InputSplit(Arrays.asList(new InputInfo(line, 0, -1)));
                                        paths.add(temp);
                                }

                                tableToInputSplits.put(tableName, paths);
                        } catch (Exception e) {
                                e.printStackTrace();
                        }
                }
                // for(String key: Arrays.asList("customer", "orders", "lineitem")){
                //         System.out.println(key + " : " + tableToInputSplits.get(key).size());
                // }
                // Stage one invoke brocastJOIN and scan
                System.out.println(tableToInputSplits.get("customer").size());
                boolean stageOne = FirstStage(tableToInputSplits, 20);

                HashMap<String, List<String>> stage2Files = new HashMap<String, List<String>>();
                for (String partname : Arrays.asList("customer_partition", "orders_partition", "partitionoutput2", "scanoutput")) {
                        try {
                                List<String> filePath = new ArrayList<String>();
                                String storagepath = "s3://jingrong-lambda-test/unit_tests/intermediate_result/" + partname + "/";
                                // System.out.println("storagepath: " + storagepath);
                                Storage storage = StorageFactory.Instance().getStorage(storagepath);
                                filePath = storage.listPaths(storagepath);
                                stage2Files.put(partname, filePath);
                        } catch (Exception e) {
                                e.printStackTrace();
                        }
                }
                // boolean stageTwo = SecondStage(stage2Files, 20);

                if(stageOne){
                        System.out.println("Stage one success!");
                        // stageTwo = SecondStage(stage2Files, 20);
                }
                else{
                        System.out.println("Stage one failed!");
                        return;
                }

                // if(stageTwo){

                //         System.out.println("Stage two success!");

                // }

                // System.out.println(tableToInputSplits.size());

        }

        @Test
        public void TestFusionScan() {
                String customerFilter = "{\"schemaName\":\"tpch\",\"tableName\":\"customer\",\"columnFilters\":{}}";
                String ordersFilter = "{\"schemaName\":\"tpch\",\"tableName\":\"orders\",\"columnFilters\":{}}";
                String lineitemFilter = "{\"schemaName\":\"tpch\",\"tableName\":\"lineitem\",\"columnFilters\":{}}";

                JoinScanFusionInput joinScanInput = new JoinScanFusionInput();

                BroadcastTableInfo customer = new BroadcastTableInfo();
                customer.setColumnsToRead(new String[] { "c_name", "c_custkey" });
                customer.setKeyColumnIds(new int[] { 1 });
                customer.setTableName("customer");
                customer.setBase(true);
                customer.setInputSplits(Arrays.asList(
                                new InputSplit(Arrays.asList(new InputInfo(
                                                "s3://jingrong-lambda-test/tpch/customer/v-0-ordered/20230425092143_0.pxl", 0,
                                                8)))));
                customer.setFilter(customerFilter);
                customer.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null, null));
                joinScanInput.setSmallTable(customer);

                BroadcastTableInfo orders = new BroadcastTableInfo();
                orders.setColumnsToRead(new String[] { "o_orderkey", "o_orderdate", "o_totalprice", "o_custkey" });
                orders.setKeyColumnIds(new int[] { 3 });
                orders.setTableName("orders");
                orders.setBase(true);
                orders.setInputSplits(Arrays.asList(
                                new InputSplit(Arrays.asList(new InputInfo(
                                                "s3://jingrong-lambda-test/tpch/orders/v-0-ordered/20230425100657_1.pxl", 0,
                                                8)))));
                orders.setFilter(ordersFilter);
                orders.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null, null));
                joinScanInput.setLargeTable(orders);

                JoinInfo joinInfo = new JoinInfo();
                joinInfo.setJoinType(JoinType.EQUI_INNER);
                joinInfo.setSmallProjection(new boolean[] { true, true });
                joinInfo.setLargeProjection(new boolean[] { true, true, true, false });
                joinInfo.setSmallColumnAlias(new String[] { "c_name", "c_custkey" });
                joinInfo.setLargeColumnAlias(new String[] { "o_orderkey", "o_orderdate", "o_totalprice" });
                PartitionInfo postPartitionInfo = new PartitionInfo();
                postPartitionInfo.setKeyColumnIds(new int[] { 2 });
                postPartitionInfo.setNumPartition(20);
                joinInfo.setPostPartition(true);
                joinInfo.setPostPartitionInfo(postPartitionInfo);
                joinScanInput.setJoinInfo(joinInfo);

                PartitionInput rightPartitionInfo = new PartitionInput();
                rightPartitionInfo.setTransId(123456);
                ScanTableInfo tableInfo = new ScanTableInfo();
                tableInfo.setTableName("lineitem");
                tableInfo.setInputSplits(Arrays.asList(
                                new InputSplit(Arrays.asList(new InputInfo(
                                                "s3://jingrong-lambda-test/tpch/lineitem/v-0-ordered/20230425092344_47.pxl", 0,
                                                -1),
                                                new InputInfo("s3://jingrong-lambda-test/tpch/lineitem/v-0-ordered/20230425092347_48.pxl",
                                                                0, -1)))));
                tableInfo.setColumnsToRead(new String[] { "l_orderkey", "l_quantity" });
                tableInfo.setFilter(lineitemFilter);
                tableInfo.setBase(true);
                tableInfo.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null, null));
                rightPartitionInfo.setTableInfo(tableInfo);
                rightPartitionInfo.setProjection(new boolean[] { true, true });
                PartitionInfo partitionInfo = new PartitionInfo();
                partitionInfo.setKeyColumnIds(new int[] { 0 });
                partitionInfo.setNumPartition(20);
                rightPartitionInfo.setPartitionInfo(partitionInfo);

                joinScanInput.setPartitionlargeTable(rightPartitionInfo);
                ScanPipeInfo scanPipeInfo = new ScanPipeInfo();
                scanPipeInfo.setIncludeCols(new String[] { "l_orderkey", "l_quantity" });
                scanPipeInfo.setRootTableName("lineitem");
                // 1.project
                LogicalProject logicalProject = new LogicalProject(new String[] { "l_orderkey", "l_quantity" },
                                new int[] { 0, 4 });
                scanPipeInfo.addOperation(logicalProject);
                // 2.aggregate

                LogicalAggregate logicalAggregate = new LogicalAggregate("SUM", "DECIMAL", new int[] { 0 },
                                new int[] { 1 });
                logicalAggregate.setGroupKeyColumnNames(new String[] { "l_orderkey" });
                logicalAggregate.setGroupKeyColumnAlias(new String[] { "l_orderkey" });
                logicalAggregate.setResultColumnAlias(new String[] { "SUM_l_quantity" });
                logicalAggregate.setResultColumnTypes(new String[] { "DECIMAL" });
                logicalAggregate.setPartition(false);
                logicalAggregate.setNumPartition(0);
                logicalAggregate.setFunctionTypes(new FunctionType[] { FunctionType.SUM });

                scanPipeInfo.addOperation(logicalAggregate);
                joinScanInput.setScanPipelineInfo(scanPipeInfo);

                // set fusionOutput
                MultiOutputInfo fusionOutput = new MultiOutputInfo();
                fusionOutput.setPath("s3://jingrong-lambda-test/unit_tests/intermediate_result/");
                fusionOutput.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null, null));
                fusionOutput.setEncoding(false);
                fusionOutput.setFileNames(new ArrayList<String>(
                                Arrays.asList("partitionoutput1", "partitionoutput2", "scanoutput")));
                joinScanInput.setFusionOutput(fusionOutput);

                // System.out.println(JSON.toJSONString(joinScanInput));
                WorkerMetrics workerMetrics = new WorkerMetrics();
                Logger logger = LogManager.getLogger(AppTest.class);
                WorkerContext workerContext = new WorkerContext(logger, workerMetrics, "123456");
                BaseJoinScanFusionWorker baseWorker = new BaseJoinScanFusionWorker(workerContext);
                FusionOutput result = (FusionOutput) baseWorker.process(joinScanInput);
                // System.out.println(JSON.toJSONString(result));

        }

        @Test
        public void ThreadAggregationFilter() {
                String filter1 = "{\"schemaName\":\"tpch\",\"tableName\":\"orders\"," +
                                "\"columnFilters\":{3:{\"columnName\":\"o_orderpriority\"," +
                                "\"columnType\":\"CHAR\",\"filterJson\":\"{\\\"javaType\\\":" +
                                "\\\"java.lang.String\\\",\\\"isAll\\\":false,\\\"isNone\\\":false," +
                                "\\\"allowNull\\\":false,\\\"onlyNull\\\":false,\\\"ranges\\\":[]," +
                                "\\\"discreteValues\\\":[{\\\"type\\\":\\\"INCLUDED\\\"," +
                                "\\\"value\\\":\\\"3-MEDIUM\\\"}]}\"}}}";

                String filter2 = "{\"schemaName\":\"tpch\",\"tableName\":\"orders\"," +
                                "\"columnFilters\":{2:{\"columnName\":\"o_orderstatus\"," +
                                "\"columnType\":\"CHAR\",\"filterJson\":\"{\\\"javaType\\\":" +
                                "\\\"java.lang.String\\\",\\\"isAll\\\":false,\\\"isNone\\\":false," +
                                "\\\"allowNull\\\":false,\\\"onlyNull\\\":false,\\\"ranges\\\":[]," +
                                "\\\"discreteValues\\\":[{\\\"type\\\":\\\"INCLUDED\\\"," +
                                "\\\"value\\\":\\\"P\\\"}]}\"}}}";

                List<String> filterlist = Arrays.asList(filter1, filter2);

                ThreadScanInput scaninput = new ThreadScanInput();
                scaninput.setTransId(123456);
                ThreadScanTableInfo tableInfo = new ThreadScanTableInfo();
                tableInfo.setTableName("orders");

                List<InputSplit> myList = new ArrayList<InputSplit>();
                try {
                        List<String> allLines = Files
                                        .readAllLines(Paths.get("/home/ubuntu/opt/lambda-java8/orders-url-small.txt"));

                        for (String line : allLines) {
                                InputSplit temp = new InputSplit(Arrays.asList(new InputInfo(line, 0, -1)));
                                myList.add(temp);
                        }
                } catch (IOException e) {
                        e.printStackTrace();
                }
                tableInfo.setInputSplits(myList);
                tableInfo.setColumnsToRead(new String[] { "o_orderkey", "o_custkey", "o_orderstatus", "o_orderpriority",
                                "o_totalprice" });
                HashMap<String, List<Boolean>> scanProjection = new HashMap<String, List<Boolean>>();
                scanProjection.put("0", Arrays.asList(false, true, false, false, true));
                scanProjection.put("1", Arrays.asList(false, true, false, false, false));

                scaninput.setScanProjection(scanProjection);
                scaninput.setPartialAggregationPresent(true);

                // here the filter more like for projection
                // HashMap<String, List<String>> filterToRead = new HashMap<String,
                // List<String>>();
                // filterToRead.put("0", Arrays.asList("o_custkey","o_orderpriority",
                // "o_totalprice"));
                // // filterToRead.put("0", Arrays.asList("o_custkey"));
                // filterToRead.put("1", Arrays.asList("o_custkey"));
                // tableInfo.setFilterToRead(filterToRead);

                tableInfo.setFilter(filterlist);
                tableInfo.setBase(true);
                tableInfo.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null, null));
                scaninput.setTableInfo(tableInfo);

                List<PartialAggregationInfo> aggregationlist = new ArrayList<>();

                // aggregation1
                PartialAggregationInfo aggregationInfo1 = new PartialAggregationInfo();
                aggregationInfo1.setAggregateColumnIds(new int[] { 1 });
                aggregationInfo1.setFunctionTypes(new FunctionType[] { FunctionType.SUM });
                aggregationInfo1.setGroupKeyColumnAlias(new String[] { "o_custkey" });
                aggregationInfo1.setGroupKeyColumnIds(new int[] { 0 });
                aggregationInfo1.setNumPartition(0);
                aggregationInfo1.setPartition(false);
                aggregationInfo1.setResultColumnAlias(new String[] { "num" });
                aggregationInfo1.setResultColumnTypes(new String[] { "DECIMAL" });
                // aggregation 2
                PartialAggregationInfo aggregationInfo2 = new PartialAggregationInfo();
                aggregationInfo2.setAggregateColumnIds(new int[] { 0 });
                aggregationInfo2.setFunctionTypes(new FunctionType[] { FunctionType.COUNT });
                aggregationInfo2.setGroupKeyColumnAlias(new String[] { "o_custkey" });
                aggregationInfo2.setGroupKeyColumnIds(new int[] { 0 });
                aggregationInfo2.setNumPartition(0);
                aggregationInfo2.setPartition(false);
                aggregationInfo2.setResultColumnAlias(new String[] { "num" });
                aggregationInfo2.setResultColumnTypes(new String[] { "bigint" });
                //
                aggregationlist.add(aggregationInfo1);
                aggregationlist.add(aggregationInfo2);
                scaninput.setPartialAggregationInfo(aggregationlist);
                List<String> list = new ArrayList<String>();
                list.add("s3://jingrong-lambda-test/unit_tests/test_scan1/");
                list.add("s3://jingrong-lambda-test/unit_tests/test_scan2/");

                // randomwfilename shouold be false?????
                ThreadOutputInfo threadoutput = new ThreadOutputInfo(list, false,
                                new StorageInfo(Storage.Scheme.s3, null, null, null, null), true);
                scaninput.setOutput(threadoutput);
                HashMap<String, List<Integer>> filterOnAggreation = new HashMap<String, List<Integer>>();
                filterOnAggreation.put("0", Arrays.asList(0));
                filterOnAggreation.put("1", Arrays.asList(1));
                scaninput.setFilterOnAggreation(filterOnAggreation);

                System.out.println(JSON.toJSONString(scaninput));

                // InvokerFactory invokerFactory = InvokerFactory.Instance();
                // try{
                // Output result =(Output)
                // invokerFactory.getInvoker(WorkerType.THREAD_SCAN).invoke(scaninput).get();
                // System.out.println(JSON.toJSONString(result));
                // }catch(Exception e){
                // e.printStackTrace();
                // }

                System.out.println(JSON.toJSONString(scaninput));
                // WorkerMetrics workerMetrics = new WorkerMetrics();
                // Logger logger = LoggerFactory.getLogger(AppTest.class);
                // WorkerContext workerContext = new WorkerContext(logger, workerMetrics,
                // "123456");
                // BaseThreadScanWorker baseWorker = new BaseThreadScanWorker(workerContext);
                // Output result =(Output) baseWorker.process(scaninput);
                // System.out.println(JSON.toJSONString(result));
        }

        @Test
        public void ThreadAggre() {
                String filter1 = "{\"schemaName\":\"tpch\",\"tableName\":\"orders\"," +
                                "\"columnFilters\":{3:{\"columnName\":\"o_orderpriority\"," +
                                "\"columnType\":\"CHAR\",\"filterJson\":\"{\\\"javaType\\\":" +
                                "\\\"java.lang.String\\\",\\\"isAll\\\":false,\\\"isNone\\\":false," +
                                "\\\"allowNull\\\":false,\\\"onlyNull\\\":false,\\\"ranges\\\":[]," +
                                "\\\"discreteValues\\\":[{\\\"type\\\":\\\"INCLUDED\\\"," +
                                "\\\"value\\\":\\\"3-MEDIUM\\\"}]}\"}}}";

                String filter2 = "{\"schemaName\":\"tpch\",\"tableName\":\"orders\"," +
                                "\"columnFilters\":{2:{\"columnName\":\"o_orderstatus\"," +
                                "\"columnType\":\"CHAR\",\"filterJson\":\"{\\\"javaType\\\":" +
                                "\\\"java.lang.String\\\",\\\"isAll\\\":false,\\\"isNone\\\":false," +
                                "\\\"allowNull\\\":false,\\\"onlyNull\\\":false,\\\"ranges\\\":[]," +
                                "\\\"discreteValues\\\":[{\\\"type\\\":\\\"INCLUDED\\\"," +
                                "\\\"value\\\":\\\"P\\\"}]}\"}}}";

                List<String> filterlist = Arrays.asList(filter1, filter2);

                ThreadScanInput scaninput = new ThreadScanInput();
                scaninput.setTransId(123456);
                ThreadScanTableInfo tableInfo = new ThreadScanTableInfo();
                tableInfo.setTableName("orders");

                List<InputSplit> myList = new ArrayList<InputSplit>();
                try {
                        List<String> allLines = Files.readAllLines(
                                        Paths.get("/home/ubuntu/opt/pixels/pixels-experiments/orders-url-small.txt"));

                        for (String line : allLines) {
                                InputSplit temp = new InputSplit(Arrays.asList(new InputInfo(line, 0, -1)));
                                myList.add(temp);
                        }
                } catch (IOException e) {
                        e.printStackTrace();
                }
                tableInfo.setInputSplits(myList);

                tableInfo.setColumnsToRead(new String[] { "o_orderkey", "o_custkey", "o_orderstatus", "o_orderpriority",
                                "o_totalprice" });
                tableInfo.setFilter(filterlist);
                tableInfo.setBase(true);
                tableInfo.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null, null));
                scaninput.setTableInfo(tableInfo);

                // scaninput.setScanProjection(new boolean[]{true, true, true, true, true});
                scaninput.setPartialAggregationPresent(false);

                List<PartialAggregationInfo> aggregationlist = new ArrayList<>();
                // aggregation1
                PartialAggregationInfo aggregationInfo1 = new PartialAggregationInfo();
                aggregationInfo1.setAggregateColumnIds(new int[] { 4 });
                aggregationInfo1.setFunctionTypes(new FunctionType[] { FunctionType.SUM });
                aggregationInfo1.setGroupKeyColumnAlias(new String[] { "o_custkey_agg" });
                aggregationInfo1.setGroupKeyColumnIds(new int[] { 1 });
                aggregationInfo1.setNumPartition(0);
                aggregationInfo1.setPartition(false);
                aggregationInfo1.setResultColumnAlias(new String[] { "num_agg" });
                aggregationInfo1.setResultColumnTypes(new String[] { "bigint" });
                // aggregation 2
                PartialAggregationInfo aggregationInfo2 = new PartialAggregationInfo();
                aggregationInfo2.setAggregateColumnIds(new int[] { 0 });
                aggregationInfo2.setFunctionTypes(new FunctionType[] { FunctionType.COUNT });
                aggregationInfo2.setGroupKeyColumnAlias(new String[] { "o_custkey_agg" });
                aggregationInfo2.setGroupKeyColumnIds(new int[] { 0 });
                aggregationInfo2.setNumPartition(0);
                aggregationInfo2.setPartition(false);
                aggregationInfo2.setResultColumnAlias(new String[] { "num_agg" });
                aggregationInfo2.setResultColumnTypes(new String[] { "bigint" });
                //
                aggregationlist.add(aggregationInfo1);
                aggregationlist.add(aggregationInfo2);
                scaninput.setPartialAggregationInfo(aggregationlist);
                List<String> list = new ArrayList<String>();
                list.add("s3://jingrong-lambda-test/unit_tests/test_scan1/");
                list.add("s3://jingrong-lambda-test/unit_tests/test_scan2/");

                // randomwfilename shouold be false?????
                ThreadOutputInfo threadoutput = new ThreadOutputInfo(list, false,
                                new StorageInfo(Storage.Scheme.s3, null, null, null, null), true);
                scaninput.setOutput(threadoutput);
                HashMap<String, List<Integer>> filterOnAggreation = new HashMap<String, List<Integer>>();
                filterOnAggreation.put("0", Arrays.asList(0));
                filterOnAggreation.put("1", Arrays.asList(1));
                scaninput.setFilterOnAggreation(filterOnAggreation);

                System.out.println(JSON.toJSONString(scaninput));

                // InvokerFactory invokerFactory = InvokerFactory.Instance();
                // try{
                // Output result =(Output)
                // invokerFactory.getInvoker(WorkerType.SCAN).invoke(scaninput).get();
                // System.out.println(JSON.toJSONString(result));
                // }catch(Exception e){
                // e.printStackTrace();
                // }

                // System.out.println(JSON.toJSONString(scaninput));
                // WorkerMetrics workerMetrics = new WorkerMetrics();
                // Logger logger = LoggerFactory.getLogger(AppTest.class);
                // WorkerContext workerContext = new WorkerContext(logger, workerMetrics,
                // "123456");
                // BaseThreadScanWorker baseWorker = new BaseThreadScanWorker(workerContext);
                // baseWorker.process(scaninput);

        }

        @Test
        public void BaseResponse() {
                String filter1 = "{\"schemaName\":\"tpch\",\"tableName\":\"orders\"," +
                                "\"columnFilters\":{1:{\"columnName\":\"o_custkey\",\"columnType\":\"LONG\"," +
                                "\"filterJson\":\"{\\\"javaType\\\":\\\"long\\\",\\\"isAll\\\":false," +
                                "\\\"isNone\\\":false,\\\"allowNull\\\":false,\\\"ranges\\\":[{" +
                                "\\\"lowerBound\\\":{\\\"type\\\":\\\"UNBOUNDED\\\"}," +
                                "\\\"upperBound\\\":{\\\"type\\\":\\\"INCLUDED\\\",\\\"value\\\":100}}," +
                                "{\\\"lowerBound\\\":{\\\"type\\\":\\\"EXCLUDED\\\",\\\"value\\\":200}," +
                                "\\\"upperBound\\\":{\\\"type\\\":\\\"UNBOUNDED\\\"}}]," +
                                "\\\"discreteValues\\\":[]}\"}}}";
                List<InputSplit> myList = new ArrayList<InputSplit>();
                try {
                        List<String> allLines = Files.readAllLines(
                                        Paths.get("/home/ubuntu/opt/pixels/pixels-experiments/orders-url-small.txt"));

                        for (String line : allLines) {
                                InputSplit temp = new InputSplit(Arrays.asList(new InputInfo(line, 0, -1)));
                                myList.add(temp);
                        }
                } catch (IOException e) {
                        e.printStackTrace();
                }

                ScanInput scaninput = new ScanInput();
                scaninput.setTransId(123456);
                ScanTableInfo tableInfo = new ScanTableInfo();
                tableInfo.setTableName("orders");

                tableInfo.setInputSplits(myList);
                tableInfo.setColumnsToRead(new String[] { "o_orderkey", "o_custkey", "o_orderstatus", "o_orderdate" });
                tableInfo.setFilter(filter1);
                tableInfo.setBase(true);
                tableInfo.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null, null));
                scaninput.setTableInfo(tableInfo);
                scaninput.setScanProjection(new boolean[] { true, true, true, true });

                scaninput.setOutput(new OutputInfo("s3://jingrong-lambda-test/unit_tests/test_scan1",
                                new StorageInfo(Storage.Scheme.s3, null, null, null, null), true));

                System.out.println(JSON.toJSONString(scaninput));

                WorkerMetrics workerMetrics = new WorkerMetrics();
                Logger logger = LogManager.getLogger(AppTest.class);
                WorkerContext workerContext = new WorkerContext(logger, workerMetrics, "123456");
                BaseScanWorker baseWorker = new BaseScanWorker(workerContext);
                baseWorker.process(scaninput);
        }

        @Test
        public void successfulResponse() {
                // App app = new App();

                // String filter1 =
                // "{\"schemaName\":\"tpch\",\"tableName\":\"orders\"," +
                // "\"columnFilters\":{1:{\"columnName\":\"o_custkey\",\"columnType\":\"LONG\","
                // +
                // "\"filterJson\":\"{\\\"javaType\\\":\\\"long\\\",\\\"isAll\\\":false," +
                // "\\\"isNone\\\":false,\\\"allowNull\\\":false,\\\"ranges\\\":[{" +
                // "\\\"lowerBound\\\":{\\\"type\\\":\\\"UNBOUNDED\\\"}," +
                // "\\\"upperBound\\\":{\\\"type\\\":\\\"INCLUDED\\\",\\\"value\\\":100}}," +
                // "{\\\"lowerBound\\\":{\\\"type\\\":\\\"EXCLUDED\\\",\\\"value\\\":200}," +
                // "\\\"upperBound\\\":{\\\"type\\\":\\\"UNBOUNDED\\\"}}]," +
                // "\\\"discreteValues\\\":[]}\"}}}";
                // String filter2 =
                // "{\"schemaName\":\"tpch\",\"tableName\":\"orders\"," +
                // "\"columnFilters\":{1:{\"columnName\":\"o_orderkey\",\"columnType\":\"LONG\","
                // +
                // "\"filterJson\":\"{\\\"javaType\\\":\\\"long\\\",\\\"isAll\\\":false," +
                // "\\\"isNone\\\":false,\\\"allowNull\\\":false,\\\"ranges\\\":[{" +
                // "\\\"lowerBound\\\":{\\\"type\\\":\\\"UNBOUNDED\\\"}," +
                // "\\\"upperBound\\\":{\\\"type\\\":\\\"INCLUDED\\\",\\\"value\\\":100}}," +
                // "{\\\"lowerBound\\\":{\\\"type\\\":\\\"EXCLUDED\\\",\\\"value\\\":200}," +
                // "\\\"upperBound\\\":{\\\"type\\\":\\\"UNBOUNDED\\\"}}]," +
                // "\\\"discreteValues\\\":[]}\"}}}";
                // List<String> filterlist=Arrays.asList(filter1,filter2);

                // ThreadScanInput scaninput = new ThreadScanInput();
                // scaninput.setQueryId(123456);
                // ThreadScanTableInfo tableInfo = new ThreadScanTableInfo();
                // tableInfo.setTableName("orders");

                // List<InputSplit> myList = new ArrayList<InputSplit>();
                // try {
                // List<String> allLines =
                // Files.readAllLines(Paths.get("/home/ubuntu/opt/pixels/pixels-experiments/orders-url-small.txt"));

                // for (String line : allLines) {
                // InputSplit temp= new InputSplit(Arrays.asList(new InputInfo(line, 0, -1)));
                // myList.add(temp);
                // }
                // } catch (IOException e) {
                // e.printStackTrace();
                // }
                // tableInfo.setInputSplits(myList);

                // // tableInfo.setInputSplits(Arrays.asList(
                // // new InputSplit(Arrays.asList(new
                // InputInfo("jingrong-lambda-test/orders/v-0-ordered/20230425100700_2.pxl", 0, -1)))));

                // tableInfo.setColumnsToRead(new String[]{"o_orderkey", "o_custkey",
                // "o_orderstatus", "o_orderdate"});
                // tableInfo.setFilter(filterlist);
                // tableInfo.setBase(true);
                // tableInfo.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null,
                // null));
                // scaninput.setTableInfo(tableInfo);
                // scaninput.setScanProjection(new boolean[]{true, true, true, true});

                // List<String> list=new ArrayList<String>();
                // list.add("s3://jingrong-lambda-test/unit_tests/test_scan1/");
                // list.add("s3://jingrong-lambda-test/unit_tests/test_scan2/");
                // ThreadOutputInfo threadoutput = new ThreadOutputInfo(list, true,
                // new StorageInfo(Storage.Scheme.s3, null, null, null), true);

                // scaninput.setOutput(threadoutput);
                // WorkerMetrics workerMetrics = new WorkerMetrics();

                // Logger logger = LoggerFactory.getLogger(AppTest.class);
                // WorkerContext workerContext = new WorkerContext(logger, workerMetrics,
                // "123456");
                // BaseThreadScanWorker baseWorker = new BaseThreadScanWorker(workerContext);
                // baseWorker.process(scaninput);

                // APIGatewayProxyResponseEvent result = app.handleRequest(null, null);
                // assertEquals(200, result.getStatusCode().intValue());
                // assertEquals("application/json", result.getHeaders().get("Content-Type"));
                // String content = result.getBody();
                // assertNotNull(content);
                // assertTrue(content.contains("\"message\""));
                // assertTrue(content.contains("\"hello world\""));
                // assertTrue(content.contains("\"location\""));
                // System.out.println("testasasf");
        }
}

// public void backup(){
//         String customerFilter = "{\"schemaName\":\"tpch\",\"tableName\":\"customer\",\"columnFilters\":{}}";
//                 String ordersFilter = "{\"schemaName\":\"tpch\",\"tableName\":\"orders\",\"columnFilters\":{}}";
//                 String lineitemFilter = "{\"schemaName\":\"tpch\",\"tableName\":\"lineitem\",\"columnFilters\":{}}";
//                 JoinScanFusionInput joinScanInput = new JoinScanFusionInput();
//                 BroadcastTableInfo customer = new BroadcastTableInfo();
//                 customer.setColumnsToRead(new String[] { "c_name", "c_custkey" });
//                 customer.setKeyColumnIds(new int[] { 1 });
//                 customer.setTableName("customer");
//                 customer.setBase(true);
//                 List<InputSplit> customerInputSplit = tableToInputSplits.get("customer");

//                 // smaller splits


//                 // smaller splits

//                 customer.setInputSplits(customerInputSplit);
//                 customer.setFilter(customerFilter);
//                 customer.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null, null));
//                 joinScanInput.setSmallTable(customer);

//                 BroadcastTableInfo orders = new BroadcastTableInfo();
//                 orders.setColumnsToRead(new String[] { "o_orderkey", "o_orderdate", "o_totalprice", "o_custkey" });
//                 orders.setKeyColumnIds(new int[] { 3 });
//                 orders.setTableName("orders");
//                 orders.setBase(true);
//                 // System.out.println("here still fine");

//                 int o_NumOfFile = (int)Math.ceil((double)tableToInputSplits.get("orders").size()/(double)CFnumber);
//                 List<InputSplit> ordersInputSplit = tableToInputSplits.get("orders").subList(num*o_NumOfFile, (num+1)*o_NumOfFile > tableToInputSplits.get("orders").size() ? tableToInputSplits.get("orders").size() : (num+1)*o_NumOfFile);
//                 orders.setInputSplits(ordersInputSplit);
//                 orders.setFilter(ordersFilter);
//                 orders.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null, null));
//                 joinScanInput.setLargeTable(orders);

//                 JoinInfo joinInfo = new JoinInfo();
//                 joinInfo.setJoinType(JoinType.EQUI_INNER);
//                 joinInfo.setSmallProjection(new boolean[] { true, true });
//                 joinInfo.setLargeProjection(new boolean[] { true, true, true, false });
//                 joinInfo.setSmallColumnAlias(new String[] { "c_name", "c_custkey" });
//                 joinInfo.setLargeColumnAlias(new String[] { "o_orderkey", "o_orderdate", "o_totalprice" });
//                 PartitionInfo postPartitionInfo = new PartitionInfo();
//                 postPartitionInfo.setKeyColumnIds(new int[] { 2 });
//                 postPartitionInfo.setNumPartition(20);
//                 joinInfo.setPostPartition(true);
//                 joinInfo.setPostPartitionInfo(postPartitionInfo);
//                 joinScanInput.setJoinInfo(joinInfo);

//                 PartitionInput rightPartitionInfo = new PartitionInput();
//                 rightPartitionInfo.setTransId(123456);
//                 ScanTableInfo tableInfo = new ScanTableInfo();
//                 tableInfo.setTableName("lineitem");
//                 Integer l_NumOfFile = (int)Math.ceil((double)tableToInputSplits.get("lineitem").size()/(double)CFnumber);
//                 List<InputSplit> lineitemInputSplit = tableToInputSplits.get("lineitem").subList(num*l_NumOfFile, (num+1)*l_NumOfFile > tableToInputSplits.get("lineitem").size() ? tableToInputSplits.get("lineitem").size() : (num+1)*l_NumOfFile);
//                 tableInfo.setInputSplits(lineitemInputSplit);
//                 tableInfo.setColumnsToRead(new String[] { "l_orderkey", "l_quantity" });
//                 tableInfo.setFilter(lineitemFilter);
//                 tableInfo.setBase(true);
//                 tableInfo.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null, null));
//                 rightPartitionInfo.setTableInfo(tableInfo);
//                 rightPartitionInfo.setProjection(new boolean[] { true, true });
//                 PartitionInfo partitionInfo = new PartitionInfo();
//                 partitionInfo.setKeyColumnIds(new int[] { 0 });
//                 partitionInfo.setNumPartition(40);
//                 rightPartitionInfo.setPartitionInfo(partitionInfo);

//                 joinScanInput.setPartitionlargeTable(rightPartitionInfo);
//                 ScanPipeInfo scanPipeInfo = new ScanPipeInfo();
//                 scanPipeInfo.setIncludeCols(new String[] { "l_orderkey", "l_quantity" });
//                 scanPipeInfo.setRootTableName("lineitem");
//                 // 1.project
//                 LogicalProject logicalProject = new LogicalProject(new String[] { "l_orderkey", "l_quantity" },
//                                 new int[] { 0, 4 });
//                 scanPipeInfo.addOperation(logicalProject);
//                 // 2.aggregate

//                 LogicalAggregate logicalAggregate = new LogicalAggregate("SUM", "DECIMAL", new int[] { 0 },
//                                 new int[] { 1 });
//                 logicalAggregate.setGroupKeyColumnNames(new String[] { "l_orderkey" });
//                 logicalAggregate.setGroupKeyColumnAlias(new String[] { "l_orderkey" });
//                 logicalAggregate.setResultColumnAlias(new String[] { "SUM_l_quantity" });
//                 logicalAggregate.setResultColumnTypes(new String[] { "DECIMAL" });
//                 logicalAggregate.setPartition(false);
//                 logicalAggregate.setNumPartition(0);
//                 logicalAggregate.setFunctionTypes(new FunctionType[] { FunctionType.SUM });

//                 scanPipeInfo.addOperation(logicalAggregate);
//                 joinScanInput.setScanPipelineInfo(scanPipeInfo);

//                 // set fusionOutput
//                 MultiOutputInfo fusionOutput = new MultiOutputInfo();
//                 fusionOutput.setPath("s3://jingrong-lambda-test/unit_tests/intermediate_result/");
//                 fusionOutput.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null, null));
//                 fusionOutput.setEncoding(false);
//                 fusionOutput.setFileNames(new ArrayList<String>(
//                                 Arrays.asList("partitionoutput1/Part_"+num, "partitionoutput2/Part_"+num, "scanoutput/Part_"+num)));
//                 joinScanInput.setFusionOutput(fusionOutput);



//                 futures.add(invokeLocalFusionJoinScan(joinScanInput));             
// }


