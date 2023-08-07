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
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.I;

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

public class BaselineTest {


        public HashMap<String, List<InputSplit>> getTableToInputSplits(List<String> tables) {
                HashMap<String, List<InputSplit>> tableToInputSplits = new HashMap<String, List<InputSplit>>();
                for (String tableName : tables) {
                try {
                        List<InputSplit> paths = new ArrayList<InputSplit>();
                        List<String> filePath = new ArrayList<String>();

                        String storagepath = "s3://jingrong-lambda-test/tpch/" + tableName + "/" + "v-0-ordered" + "/";
                        // System.out.println("storagepath: " + storagepath);
                        Storage storage = StorageFactory.Instance().getStorage(storagepath);
                        filePath = storage.listPaths(storagepath);

                        for (String line : filePath) {
                                InputSplit temp = new InputSplit(Arrays.asList(new InputInfo(line, 0, -1)));
                                paths.add(temp);
                        }

                        tableToInputSplits.put(tableName, paths);
                        
                } catch (Exception e) {
                        e.printStackTrace();
                        return null;
                }
                }
                return tableToInputSplits;
        }

    // Here contains the hard code of starling shuffle pure cloud function version
        @Test
        public void BaselineTPCHQ18(){
                System.setProperty("PIXELS_HOME", "/home/ubuntu/opt/pixels");
                boolean isLocal = false;
                String[] tables = {"customer","lineitem","orders"};
                HashMap<String, List<InputSplit>> tableToInputSplits = getTableToInputSplits(Arrays.asList(tables));
                InvokerFactory invokerFactory = InvokerFactory.Instance();


                // stage1
                ImmutableTriple<Boolean, List<PartitionOutput>,List<ScanOutput>> stage1out = BaselineFirstStage(isLocal,invokerFactory,tableToInputSplits,40,1);
                boolean stage1success = stage1out.getLeft();
                List<PartitionOutput> stage1partitionOutputs = stage1out.getMiddle();
                List<ScanOutput> stage1scanOutputs = stage1out.getRight();

                if(stage1success){
                        System.out.println("stage1 success");
                }else{
                        System.out.println("stage1 failed");
                }

                // stage2

        }

        public ImmutableTriple<Boolean, List<PartitionOutput>,List<ScanOutput>> BaselineFirstStage(boolean local,InvokerFactory invokerFactory,HashMap<String, List<InputSplit>> tableToInputSplits,int CFnumber,int testNum) {
                List<CompletableFuture<PartitionOutput>> partitionFuture = new ArrayList<CompletableFuture<PartitionOutput>>();
                List<CompletableFuture<ScanOutput>> scanfutures = new ArrayList<CompletableFuture<ScanOutput>>();
                List<CompletableFuture<Output>> CloudFuture = new ArrayList<CompletableFuture<Output>>();
                List<PartitionOutput> partitionOutputs = new ArrayList<PartitionOutput>();
                List<ScanOutput> scanOutputs = new ArrayList<ScanOutput>();
                InvokerLocal invokerLocal= new InvokerLocal();

                // partition customer
                String customerFilter = "{\"schemaName\":\"tpch\",\"tableName\":\"customer\",\"columnFilters\":{}}";
                PartitionInput input = new PartitionInput();
                input.setTransId(123456);
                ScanTableInfo tableInfo = new ScanTableInfo();
                tableInfo.setTableName("customer");
                int c_NumOfFile = (int)Math.ceil((double)tableToInputSplits.get("customer").size()/(double)CFnumber);
                
                tableInfo.setFilter(customerFilter);
                tableInfo.setBase(true);
                tableInfo.setColumnsToRead(new String[] { "c_custkey","c_name"});
                tableInfo.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null, null));
                
                input.setProjection(new boolean[] { true, true });
                PartitionInfo partitionInfo = new PartitionInfo();
                partitionInfo.setNumPartition(20);
                partitionInfo.setKeyColumnIds(new int[] {0});
                input.setPartitionInfo(partitionInfo);
                //partition orders
                String ordersFilter = "{\"schemaName\":\"tpch\",\"tableName\":\"orders\",\"columnFilters\":{}}";
                PartitionInput o_Input = new PartitionInput();
                o_Input.setTransId(123456);
                ScanTableInfo o_tableInfo = new ScanTableInfo();
                o_tableInfo.setTableName("orders");
                int o_NumOfFile = (int)Math.ceil((double)tableToInputSplits.get("orders").size()/(double)CFnumber);
                o_tableInfo.setFilter(ordersFilter);
                o_tableInfo.setBase(true);
                o_tableInfo.setColumnsToRead(new String[] { "o_orderkey","o_custkey","o_orderdate","o_totalprice"});
                o_tableInfo.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null, null));
                o_Input.setTableInfo(o_tableInfo);
                o_Input.setProjection(new boolean[] { true, true, true, true });
                PartitionInfo o_partitionInfo = new PartitionInfo();
                o_partitionInfo.setNumPartition(20);
                o_partitionInfo.setKeyColumnIds(new int[] {1});
                o_Input.setPartitionInfo(o_partitionInfo);

                // partition lineitem
                String lineitemFilter = "{\"schemaName\":\"tpch\",\"tableName\":\"lineitem\",\"columnFilters\":{}}";
                PartitionInput l_Input = new PartitionInput();
                l_Input.setTransId(123456);
                ScanTableInfo l_tableInfo = new ScanTableInfo();
                l_tableInfo.setTableName("lineitem");
                int l_NumOfFile = (int)Math.ceil((double)tableToInputSplits.get("lineitem").size()/(double)CFnumber);

                l_tableInfo.setFilter(lineitemFilter);
                l_tableInfo.setBase(true);
                l_tableInfo.setColumnsToRead(new String[] { "l_orderkey", "l_quantity"});
                l_tableInfo.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null, null));
                l_Input.setTableInfo(l_tableInfo);
                l_Input.setProjection(new boolean[] { true, true });
                PartitionInfo l_partitionInfo = new PartitionInfo();
                l_partitionInfo.setNumPartition(20);
                l_partitionInfo.setKeyColumnIds(new int[] {0});
                l_Input.setPartitionInfo(l_partitionInfo);

                for (int p_num=0;p_num<testNum;p_num++){
                // partition customer
                tableInfo.setInputSplits(tableToInputSplits.get("customer").subList(p_num*c_NumOfFile, (p_num+1)*c_NumOfFile > tableToInputSplits.get("customer").size() ? tableToInputSplits.get("customer").size() : (p_num+1)*c_NumOfFile));
                input.setTableInfo(tableInfo);
                input.setOutput(new OutputInfo("jingrong-lambda-test/unit_tests/intermediate_result/customer_partition/Part_" + p_num,
                new StorageInfo(Storage.Scheme.s3, null, null, null, null), false));
                
                
                if(local){
                        partitionFuture.add(invokerLocal.invokeLocalPartition(input));
                }
                else{
                        CloudFuture.add(InvokerCloud.invokeCloudPartition(invokerFactory,input));
                }

                // partition orders
                o_tableInfo.setInputSplits(tableToInputSplits.get("orders").subList(p_num*o_NumOfFile, (p_num+1)*o_NumOfFile > tableToInputSplits.get("orders").size() ? tableToInputSplits.get("orders").size() : (p_num+1)*o_NumOfFile));
                o_Input.setTableInfo(o_tableInfo);
                o_Input.setOutput(new OutputInfo("jingrong-lambda-test/unit_tests/intermediate_result/orders_partition/Part_" + p_num,
                new StorageInfo(Storage.Scheme.s3, null, null, null, null), false));
                
                if(local){
                        partitionFuture.add(invokerLocal.invokeLocalPartition(o_Input));
                }
                else{
                        CloudFuture.add(InvokerCloud.invokeCloudPartition(invokerFactory,o_Input));
                }

                        l_tableInfo.setInputSplits(tableToInputSplits.get("lineitem").subList(p_num*l_NumOfFile, (p_num+1)*l_NumOfFile > tableToInputSplits.get("lineitem").size() ? tableToInputSplits.get("lineitem").size() : (p_num+1)*l_NumOfFile));
                // partition lineitem
                l_tableInfo.setInputSplits(tableToInputSplits.get("lineitem").subList(p_num*l_NumOfFile, (p_num+1)*l_NumOfFile > tableToInputSplits.get("lineitem").size() ? tableToInputSplits.get("lineitem").size() : (p_num+1)*l_NumOfFile));
                l_Input.setTableInfo(l_tableInfo);
                l_Input.setOutput(new OutputInfo("jingrong-lambda-test/unit_tests/intermediate_result/lineitem_partition/Part_" + p_num,
                new StorageInfo(Storage.Scheme.s3, null, null, null, null), false));

                if(local){
                        partitionFuture.add(invokerLocal.invokeLocalPartition(l_Input));
                }
                else{
                        CloudFuture.add(InvokerCloud.invokeCloudPartition(invokerFactory,l_Input));
                        }

                }

                //Scan line item, can be less cfs


                ScanInput scanInput = new ScanInput();
                scanInput.setTransId(123456);
                ScanTableInfo scanTableInfo = new ScanTableInfo();
                scanTableInfo.setTableName("lineitem");
                
                scanTableInfo.setFilter("{\"schemaName\":\"tpch\",\"tableName\":\"lineitem\",\"columnFilters\":{}}");
                scanTableInfo.setBase(true);
                scanTableInfo.setColumnsToRead(new String[] { "l_orderkey", "l_quantity"});
                scanTableInfo.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null, null));
                
                PartialAggregationInfo parAggregationInfo = new PartialAggregationInfo();
                parAggregationInfo.setGroupKeyColumnNames(new ArrayList<String>(Arrays.asList("l_orderkey")));
                parAggregationInfo.setGroupKeyColumnAlias(new String[] { "l_orderkey" });
                parAggregationInfo.setGroupKeyColumnIds(new int[] { 0 });
                parAggregationInfo.setAggregateColumnIds(new int[] { 1 });
                parAggregationInfo.setResultColumnAlias(new String[] { "SUM_l_quantity" });
                parAggregationInfo.setResultColumnTypes(new String[] { "DECIMAL" });
                parAggregationInfo.setFunctionTypes(new FunctionType[] { FunctionType.SUM });
                parAggregationInfo.setPartition(false);
                parAggregationInfo.setNumPartition(0);
                scanInput.setPartialAggregationInfo(parAggregationInfo);
                scanInput.setScanProjection(new boolean[] { true, true });
                scanInput.setPartialAggregationPresent(true);

                int lineitemScan = 40; 
                for (int l_num=0;l_num<lineitemScan;l_num++){

                int l_scanNumOfFile = (int)Math.ceil((double)tableToInputSplits.get("lineitem").size()/(double)lineitemScan);
                scanTableInfo.setInputSplits(tableToInputSplits.get("lineitem").subList(l_num*l_scanNumOfFile, (l_num+1)*l_scanNumOfFile > tableToInputSplits.get("lineitem").size() ? tableToInputSplits.get("lineitem").size() : (l_num+1)*l_scanNumOfFile));
                scanInput.setTableInfo(scanTableInfo);
                scanInput.setOutput(new OutputInfo("jingrong-lambda-test/unit_tests/intermediate_result/lineitem_scan/Part_" + l_num,
                new StorageInfo(Storage.Scheme.s3, null, null, null, null), false));
                if(local){
                        scanfutures.add(invokerLocal.invokeLocalScan(scanInput));
                }
                else{
                        CloudFuture.add(InvokerCloud.invokeCloudScan(invokerFactory,scanInput));
                }
                }

                

                return new ImmutableTriple<Boolean, List<PartitionOutput>,List<ScanOutput>>(local,partitionOutputs,scanOutputs);

        }


        public void BaselineSecondStage(){
                



        }

}
