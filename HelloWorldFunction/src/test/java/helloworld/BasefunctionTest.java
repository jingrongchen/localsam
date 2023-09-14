package helloworld;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.util.hash.Hash;
import org.junit.Test;
import io.pixelsdb.pixels.planner.plan.physical.output.JoinOutput;

import com.alibaba.fastjson.JSON;
import java.util.concurrent.CompletableFuture;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.turbo.InvokerFactory;
import io.pixelsdb.pixels.common.turbo.Output;
import io.pixelsdb.pixels.executor.join.JoinType;
import io.pixelsdb.pixels.planner.plan.physical.domain.InputInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.MultiOutputInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.OutputInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.PartitionInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.PartitionedJoinInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.PartitionedTableInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.ScanTableInfo;
import io.pixelsdb.pixels.planner.plan.physical.input.PartitionInput;
import io.pixelsdb.pixels.planner.plan.physical.input.PartitionedJoinInput;
import io.pixelsdb.pixels.planner.plan.physical.output.PartitionOutput;
import io.pixelsdb.pixels.planner.plan.physical.domain.StorageInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.InputSplit;
import io.pixelsdb.pixels.planner.plan.physical.input.ScanInput;
import io.pixelsdb.pixels.planner.plan.physical.domain.PartialAggregationInfo;
import io.pixelsdb.pixels.executor.aggregation.FunctionType;
import io.pixelsdb.pixels.planner.plan.physical.output.ScanOutput;
import java.util.HashMap;
import io.pixelsdb.pixels.common.turbo.WorkerType;
import io.pixelsdb.pixels.planner.plan.physical.input.AggregationInput;
import io.pixelsdb.pixels.planner.plan.physical.output.AggregationOutput;
import io.pixelsdb.pixels.planner.plan.logical.operation.ScanpipeInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.AggregatedTableInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.AggregationInfo;
import io.pixelsdb.pixels.planner.plan.physical.input.BroadcastChainJoinInput;
import io.pixelsdb.pixels.planner.plan.physical.input.BroadcastJoinInput;
import io.pixelsdb.pixels.planner.plan.physical.input.JoinScanFusionInput;
import io.pixelsdb.pixels.planner.plan.physical.domain.BroadcastTableInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.JoinInfo;
public class BasefunctionTest {


        private long cumulativeComputeCostMs;
        private long cumulativeInputCostMs;
        private long cumulativeOutputCostMs;
        private long cumulativeDurationMs;
        private long cumulativeMemoryMB;

        private long cumulativeReadBytes;
        private long cumulativeWriteBytes;
        
        @Test
        public void testPartialagg(){
                String filter="{\"schemaName\":\"tpch\",\"tableName\":\"lineitem\",\"columnFilters\":{}}";

                ScanInput scanInput = new ScanInput();
                scanInput.setTransId(123456);
                ScanTableInfo scantableInfo = new ScanTableInfo();
                scantableInfo.setFilter(filter);
                scantableInfo.setTableName("lineitem");
                scantableInfo.setBase(true);
                scantableInfo.setColumnsToRead(new String[] { "l_orderkey", "l_quantity" });
                // scantableInfo.setSchemaName(filter);
                scantableInfo.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null, null));
                scanInput.setPartialAggregationPresent(true);
                scanInput.setScanProjection(new boolean[] { true, true });
                
                PartialAggregationInfo parAggregationInfo = new PartialAggregationInfo();
                parAggregationInfo.setGroupKeyColumnNames(new ArrayList<String>(Arrays.asList("l_orderkey")));
                parAggregationInfo.setGroupKeyColumnAlias(new String[] { "l_orderkey" });
                parAggregationInfo.setGroupKeyColumnIds(new int[] { 0 });
                parAggregationInfo.setAggregateColumnIds(new int[] { 1 });
                parAggregationInfo.setResultColumnAlias(new String[] {"SUM_l_quantity"});
                parAggregationInfo.setResultColumnTypes(new String[] {"bigint"});
                parAggregationInfo.setFunctionTypes(new FunctionType[] { FunctionType.SUM });
                parAggregationInfo.setPartition(true);
                parAggregationInfo.setNumPartition(20);
                

                List<InputSplit> lineitemInputSplit = new ArrayList<InputSplit>();
                lineitemInputSplit.add(new InputSplit(Arrays.asList(new InputInfo("s3://jingrong-lambda-test/tpch/lineitem/v-0-ordered/20230801085208_282.pxl", 0, -1))));
                
                scantableInfo.setInputSplits(lineitemInputSplit);
                scanInput.setPartialAggregationInfo(parAggregationInfo);
                scanInput.setTableInfo(scantableInfo);
                scanInput.setOutput(new OutputInfo("s3://jingrong-lambda-test/unit_tests/intermediate_result/Part_"+0,
                new StorageInfo(Storage.Scheme.s3, null, null, null, null), false));
                
                System.out.println(JSON.toJSONString(scanInput));

                // CloudFuture.add(InvokerCloud.invokeCloudScan(InvokerFactory.Instance(),scanInput));
                // try{
                //         Output output = InvokerLocal.invokeLocalScan(scanInput).get();
                //         System.out.println(JSON.toJSONString(output));
                // }catch(Exception e){
                //         System.out.println("failed!");
                //         return;
                // }
                
        }


 

    @Test
    public void testBaseScan(){

        HashMap<String, List<InputSplit>> lineitem = Common.getTableToInputSplits(Arrays.asList("lineitem"));

        // getTableToInputSplits();
        InvokerLocal invokerLocal= new InvokerLocal();
        for (int i = 0; i < 5; ++i)
        {
            String filter =
                    "{\"schemaName\":\"tpch\",\"tableName\":\"lineitem\",\"columnFilters\":{}}";
            ScanInput scanInput = new ScanInput();
            scanInput.setTransId(123456);
            ScanTableInfo tableInfo = new ScanTableInfo();
            tableInfo.setTableName("lineitem");
            tableInfo.setColumnsToRead(new String[]{ "l_orderkey", "l_quantity"});
            tableInfo.setFilter(filter);
            tableInfo.setBase(true);
            tableInfo.setInputSplits(lineitem.get("lineitem").subList(i*20, (i+1)*20));
            tableInfo.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null, null));
            scanInput.setTableInfo(tableInfo);
            scanInput.setScanProjection(new boolean[]{true, true});
            scanInput.setPartialAggregationPresent(true);
            
            PartialAggregationInfo aggregationInfo = new PartialAggregationInfo();
            aggregationInfo.setGroupKeyColumnAlias(new String[]{"l_orderkey"});
            aggregationInfo.setGroupKeyColumnIds(new int[]{0});
            aggregationInfo.setAggregateColumnIds(new int[]{1});
            aggregationInfo.setResultColumnAlias(new String[]{"SUM_l_quantity"});
            aggregationInfo.setResultColumnTypes(new String[]{"bigint"});
            aggregationInfo.setFunctionTypes(new FunctionType[]{FunctionType.SUM});
            aggregationInfo.setNumPartition(20);
            aggregationInfo.setPartition(true);

            scanInput.setPartialAggregationInfo(aggregationInfo);
            scanInput.setOutput(new OutputInfo("s3://jingrong-lambda-test/unit_tests/intermediate_result/testlineitem/part" + i,
                    new StorageInfo(Storage.Scheme.s3, null, null, null, null), false));
            
            try{
                ScanOutput output = (ScanOutput) InvokerFactory.Instance()
                        .getInvoker(WorkerType.SCAN).invoke(scanInput).get();
                System.out.println(JSON.toJSONString(output));
            }catch(Exception e){
                System.out.println("failed!");
                return;
            }

        }
    }

    @Test
    public void testfullAggregation(){
        HashMap<String, List<String>> stage1Files = Common.getTableToString("s3://jingrong-lambda-test/unit_tests/intermediate_result/",Arrays.asList("testlineitem"));
        
        AggregationInput aggregationInput = new AggregationInput();
        aggregationInput.setTransId(123456);
        AggregatedTableInfo aggregatedTableInfo = new AggregatedTableInfo();
        aggregatedTableInfo.setParallelism(3);
        aggregatedTableInfo.setStorageInfo(new StorageInfo(Storage.Scheme.s3,
        null, null, null, null));
        aggregatedTableInfo.setInputFiles(stage1Files.get("testlineitem"));
        aggregatedTableInfo.setColumnsToRead(new String[] {"l_orderkey", "SUM_l_quantity"});
        aggregatedTableInfo.setBase(false);
        aggregatedTableInfo.setTableName("aggregate_lineitem");
        aggregationInput.setAggregatedTableInfo(aggregatedTableInfo);
        

        AggregationInfo aggregationInfo = new AggregationInfo();
        aggregationInfo.setHashValues(new ArrayList<Integer>(Arrays.asList(0)));
        aggregationInfo.setGroupKeyColumnIds(new int[] {0});
        aggregationInfo.setAggregateColumnIds(new int[] {1});
        aggregationInfo.setGroupKeyColumnNames(new String[] {"l_orderkey"});
        aggregationInfo.setGroupKeyColumnProjection(new boolean[] {true});
        // aggregationInfo.setResultco
        aggregationInfo.setResultColumnNames(new String[] {"SUM_l_quantity"});
        aggregationInfo.setResultColumnTypes(new String[] {"bigint"});
        aggregationInfo.setFunctionTypes(new FunctionType[] {FunctionType.SUM});
        aggregationInfo.setNumPartition(20);
        aggregationInfo.setInputPartitioned(true);
        aggregationInput.setAggregationInfo(aggregationInfo);

        aggregationInput.setOutput(new OutputInfo("jingrong-lambda-test/unit_tests/intermediate_result/aggregation_result/final_agg",
        new StorageInfo(Storage.Scheme.s3, null, null, null, null), false));
        InvokerLocal invokerLocal= new InvokerLocal();
        try{
            Output out = invokerLocal.invokeLocalAggregation(aggregationInput).get();
            System.out.println(JSON.toJSONString(out));
        }catch(Exception e){
            System.out.println("failed!");
            return;
        }
       
        
        
        // if(islocal){
        //         aggFutures.add(InvokerLocal.invokeLocalAggregation(aggregationInput));
        // }
        // else{
        //         Cloudfutures.add(InvokerCloud.invokeCloudAggregation(InvokerFactory.Instance(),aggregationInput));
        // }


    }

    @Test
    public void testBasePartitionjoin(){
                List<Integer> hashValues = new ArrayList<Integer>();
                for (int i = 0 ; i < 30; ++i)
                {
                    hashValues.add(i);
                }

                PartitionedJoinInput joinInput = new PartitionedJoinInput();
                joinInput.setTransId(123456);

                PartitionedTableInfo leftTableInfo = new PartitionedTableInfo();
                leftTableInfo.setTableName("customer");
                leftTableInfo.setColumnsToRead(new String[]{"c_custkey", "c_name"});
                leftTableInfo.setKeyColumnIds(new int[]{0});
                leftTableInfo.setInputFiles(Arrays.asList("s3://jingrong-lambda-test/unit_tests/intermediate_result/customer_partition/Part_0"));
                leftTableInfo.setParallelism(20);
                leftTableInfo.setBase(false);
                leftTableInfo.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null, null));
                joinInput.setSmallTable(leftTableInfo);

                PartitionedTableInfo rightTableInfo = new PartitionedTableInfo();
                rightTableInfo.setTableName("orders");
                rightTableInfo.setColumnsToRead(new String[]{"o_orderkey","o_custkey","o_orderdate","o_totalprice"});
                rightTableInfo.setKeyColumnIds(new int[]{1});
                rightTableInfo.setInputFiles(Arrays.asList("s3://jingrong-lambda-test/unit_tests/intermediate_result/orders_partition/Part_0"));
                rightTableInfo.setParallelism(20);
                rightTableInfo.setBase(false);
                rightTableInfo.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null, null));
                joinInput.setLargeTable(rightTableInfo);

                PartitionedJoinInfo joinInfo = new PartitionedJoinInfo();
                joinInfo.setJoinType(JoinType.EQUI_INNER);
                joinInfo.setNumPartition(20);
                joinInfo.setHashValues(hashValues);
                // joinInfo.setHashValues(new ArrayList<Integer>(hashValues));
                joinInfo.setSmallColumnAlias(new String[]{"c_custkey", "c_name"});
                joinInfo.setLargeColumnAlias(new String[]{"o_orderkey","o_custkey","o_orderdate","o_totalprice"});
                joinInfo.setSmallProjection(new boolean[]{true, true});
                joinInfo.setLargeProjection(new boolean[]{true, true, true, true});

                joinInfo.setPostPartition(true);
                joinInfo.setPostPartitionInfo(new PartitionInfo(new int[] {2}, 30));
                joinInput.setJoinInfo(joinInfo);

                joinInput.setOutput(new MultiOutputInfo("jingrong-lambda-test/unit_tests/intermediate_result/customer_orders_partitioned/",
                new StorageInfo(Storage.Scheme.s3, null, null, null, null),
                false, Arrays.asList("partitioned_join_customer_orders"))); // force one file currently
                try{    
                        Output output=InvokerLocal.invokeLocalPartitionJoin(joinInput).get();
                        // Output output=InvokerCloud.invokeCloudPartitionJoin(InvokerFactory.Instance(), joinInput).get();
                        System.out.println(JSON.toJSONString(output));
                }catch(Exception e){  
                        System.out.println("Stage one failed!");
                        return;
                }
                
        }

    @Test
    public void testBasePartition(){
        String customerFilter = "{\"schemaName\":\"tpch\",\"tableName\":\"part\",\"columnFilters\":{}}";
        PartitionInput input = new PartitionInput();
        input.setTransId(123456);
        ScanTableInfo tableInfo = new ScanTableInfo();
        tableInfo.setTableName("part");
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
        partitionInfo.setNumPartition(20);
        partitionInfo.setKeyColumnIds(new int[] {0});
        input.setPartitionInfo(partitionInfo);
        input.setOutput(new OutputInfo("jingrong-lambda-test/unit_tests/intermediate_result/customer_partition/Part_0",
        new StorageInfo(Storage.Scheme.s3, null, null, null, null), false));
        InvokerLocal invokerLocal= new InvokerLocal();
        
        System.out.println(JSON.toJSONString(input));


        String ordersFilter = "{\"schemaName\":\"tpch\",\"tableName\":\"orders\",\"columnFilters\":{}}";
        PartitionInput input2 = new PartitionInput();
        input2.setTransId(123456);
        ScanTableInfo tableInfo2 = new ScanTableInfo();
        tableInfo2.setTableName("orders");
        tableInfo2.setInputSplits(Arrays.asList(
                    new InputSplit(Arrays.asList(new InputInfo("s3://jingrong-lambda-test/tpch/orders/v-0-ordered/20230801082419_47.pxl", 0, -1)))));
        tableInfo2.setFilter(ordersFilter);
        tableInfo2.setBase(true);
        tableInfo2.setColumnsToRead(new String[] { "o_orderkey","o_custkey","o_orderdate","o_totalprice"});
        tableInfo2.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null, null));
        input2.setTableInfo(tableInfo2);
        input2.setProjection(new boolean[] { true, true, true, true });
        PartitionInfo partitionInfo2 = new PartitionInfo();
        partitionInfo2.setNumPartition(20);
        partitionInfo2.setKeyColumnIds(new int[] {1});
        input2.setPartitionInfo(partitionInfo2);
        input2.setOutput(new OutputInfo("jingrong-lambda-test/unit_tests/intermediate_result/orders_partition/Part_0",
        new StorageInfo(Storage.Scheme.s3, null, null, null, null), false));
        


        // try {
        //         InvokerCloud invokerCloud = new InvokerCloud();

        //     Output output = InvokerCloud.invokeCloudPartition(InvokerFactory.Instance(),input).get();
        //     Output output2 = InvokerCloud.invokeCloudPartition(InvokerFactory.Instance(),input).get();
        //     System.out.println(JSON.toJSONString(output));
        //     System.out.println(JSON.toJSONString(output2));
        // } catch (Exception e) {
        //     System.out.println("Stage one failed!");
        //     return;
        // }



       


    }


    @Test
    public void testBaseBrocastingJoin(){

        boolean islocal = true;
        int brocastJoinCFnumber=80;
        int HashJoinCFnumber=80;
        List<CompletableFuture<Output>> Cloudfutures = new ArrayList<CompletableFuture<Output>>();
                
        // customer and orders join result join lineitem partitions
        List<CompletableFuture<JoinOutput>> futures = new ArrayList<CompletableFuture<JoinOutput>>();
        HashMap<String, List<String>> testfiles = Common.getTableToString("s3://jingrong-lambda-test/unit_tests/intermediate_result/", Arrays.asList("customer_orders_lineitem_partitionjoin","aggregation_result"));
        List<JoinOutput> joinOutput = new ArrayList<JoinOutput>();


        //right filter need to be changed
        String leftFilter ="{\"schemaName\":\"tpch\",\"tableName\":\"aggregate_lineitem\"," +
                        "\"columnFilters\":{1:{\"columnName\":\"SUM_l_quantity\",\"columnType\":\"LONG\"," +
                        "\"filterJson\":\"{\\\"javaType\\\":\\\"long\\\",\\\"isAll\\\":false," +
                        "\\\"isNone\\\":false,\\\"allowNull\\\":false,\\\"ranges\\\":[{" +
                        "\\\"lowerBound\\\":{\\\"type\\\":\\\"EXCLUDED\\\",\\\"value\\\":314}," +
                        "\\\"upperBound\\\":{\\\"type\\\":\\\"UNBOUNDED\\\"}}]," +
                        "\\\"discreteValues\\\":[]}\"}}}";

                
        //right filter need to be changed
        String rightFilter = "{\"schemaName\":\"tpch\",\"tableName\":\"partitioned_join_customer_orders_lineitem\",\"columnFilters\":{}}";

        BroadcastJoinInput brodcastjoinInput = new BroadcastJoinInput();
        brodcastjoinInput.setTransId(123456);
        BroadcastTableInfo leftTable = new BroadcastTableInfo();
        leftTable.setColumnsToRead(new String[] { "l_orderkey", "SUM_l_quantity" });
        leftTable.setKeyColumnIds(new int[]{0});
        leftTable.setTableName("aggregate_lineitem");
        leftTable.setBase(true);

        List<InputSplit> leftInputSplit = new ArrayList<InputSplit>();
        for(String file: testfiles.get("aggregation_result")){
                InputSplit temp = new InputSplit(Arrays.asList(new InputInfo(file, 0, 2)));
                leftInputSplit.add(temp);
        }

        leftTable.setInputSplits(leftInputSplit);
        leftTable.setFilter(leftFilter);
        leftTable.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null, null));
        brodcastjoinInput.setSmallTable(leftTable);

        BroadcastTableInfo rightTable = new BroadcastTableInfo();
        rightTable.setColumnsToRead(new String[] { "c_custkey","c_name","o_orderkey", "o_custkey", "o_orderdate","o_totalprice", "l_orderkey", "l_quantity" });
        rightTable.setKeyColumnIds(new int[]{6});
        rightTable.setTableName("partitioned_join_customer_orders_lineitem");
        rightTable.setBase(true);
        rightTable.setFilter(rightFilter);
        rightTable.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null, null));
        

        JoinInfo joinInfo2 = new JoinInfo();
        joinInfo2.setJoinType(JoinType.EQUI_INNER);
        joinInfo2.setSmallColumnAlias(new String[] { });
        joinInfo2.setLargeColumnAlias(new String[] { "c_custkey","c_name","o_orderkey", "o_orderdate","o_totalprice", "l_quantity" });
        joinInfo2.setPostPartition(false);
        joinInfo2.setSmallProjection(new boolean[] { false, false });
        joinInfo2.setLargeProjection(new boolean[] { true, true, true, false, true, true, false, true });

        List<String> secondStageFiles = testfiles.get("customer_orders_lineitem_partitionjoin");
        int fileperBr= (int)Math.ceil((double)HashJoinCFnumber/(double)brocastJoinCFnumber);
        
        for(int br_num=0;br_num<brocastJoinCFnumber;br_num++){
                List<String> local_secondStageFiles = new ArrayList<String>();
                for(int file_num=0;file_num<fileperBr;file_num++){
                        local_secondStageFiles.add(secondStageFiles.get(br_num*fileperBr+file_num));
                }
                List<InputSplit> rightInputSplit = new ArrayList<InputSplit>();
                for(String file: local_secondStageFiles){
                        InputSplit temp = new InputSplit(Arrays.asList(new InputInfo(file, 0, 1)));
                        rightInputSplit.add(temp);
                }
                // rightInputSplit.forEach(System.out::println);
                

                rightTable.setInputSplits(rightInputSplit);
                brodcastjoinInput.setLargeTable(rightTable);
                brodcastjoinInput.setJoinInfo(joinInfo2);
                brodcastjoinInput.setOutput(new MultiOutputInfo("jingrong-lambda-test/unit_tests/intermediate_result/broadcast_join/",
                new StorageInfo(Storage.Scheme.s3, null, null, null, null), false,
                Arrays.asList("part_"+br_num)));

                if(islocal){
                        futures.add(InvokerLocal.invokeLocalBrocastJoin(brodcastjoinInput));
                }
                else{
                        Cloudfutures.add(InvokerCloud.invokeCloudBrocastJoin(InvokerFactory.Instance(),brodcastjoinInput));
                }
        }

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





    }


    @Test 
    public void testBaseJoinScanFusion(){
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
        fusionpartitionInfo.setNumPartition(20);
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
        parAggregationInfo.setNumPartition(20);
        scanpipe.setPartialAggregationInfo(parAggregationInfo);
        joinScanInput.setScanPipelineInfo(scanpipe);
        // set fusionOutput
        MultiOutputInfo fusionOutput = new MultiOutputInfo();
        fusionOutput.setPath("s3://jingrong-lambda-test/unit_tests/intermediate_result/");
        fusionOutput.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null, null));
        fusionOutput.setEncoding(false);
        // int l_NumOfCF = (int)Math.ceil((double)tableToInputSplits.get("lineitem").size()/(double)filePerCf);
        // System.out.println("total size of l_numOfCF: " + l_NumOfCF);

        
        scantableInfo.setInputSplits(new ArrayList<InputSplit>(Arrays.asList(new InputSplit(Arrays.asList(new InputInfo("s3://jingrong-lambda-test/tpch/lineitem/v-0-ordered/20230801085208_282.pxl", 0, -1),
                                        new InputInfo("s3://jingrong-lambda-test/tpch/lineitem/v-0-ordered/20230801085221_286.pxl", 0, -1))))));
        rightPartitionInfo.setTableInfo(scantableInfo);
        fusionOutput.setFileNames(new ArrayList<String>(
                        Arrays.asList("partitionoutput1/Part_"+0, "lineitem_partition/Part_"+0, "scanoutput/Part_"+0)));
        joinScanInput.setFusionOutput(fusionOutput);
        joinScanInput.setPartitionlargeTable(rightPartitionInfo);
        try{
                Output result = InvokerLocal.invokeLocalFusionJoinScan(joinScanInput).get();
                cumulativeComputeCostMs += result.getCumulativeComputeCostMs();
                cumulativeInputCostMs += result.getCumulativeInputCostMs();
                cumulativeOutputCostMs += result.getCumulativeOutputCostMs();
                cumulativeDurationMs += result.getDurationMs();

                cumulativeMemoryMB += result.getMemoryMB();
                cumulativeReadBytes += result.getTotalReadBytes();
                cumulativeWriteBytes += result.getTotalWriteBytes();

                System.out.println(JSON.toJSONString(result));
        }catch(Exception e){
                System.out.println("failed!");
                return;
        }      

        System.out.println("cumulativeComputeCostMs: " + cumulativeComputeCostMs);
        System.out.println("cumulativeInputCostMs: " + cumulativeInputCostMs);
        System.out.println("cumulativeOutputCostMs: " + cumulativeOutputCostMs);
        System.out.println("cumulativeDurationMs: " + cumulativeDurationMs);
        System.out.println("cumulativeMemoryMB: " + cumulativeMemoryMB);
        System.out.println("cumulativeReadBytes: " + cumulativeReadBytes);
        System.out.println("cumulativeWriteBytes: " + cumulativeWriteBytes);

        // Output out = InvokerLocal.invokeLocalFusionJoinScan(joinScanInput).get();
    }
}
