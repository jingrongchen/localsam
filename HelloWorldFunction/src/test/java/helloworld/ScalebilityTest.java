package helloworld;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.junit.Test;

import com.alibaba.fastjson.JSON;

import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.turbo.InvokerFactory;
import io.pixelsdb.pixels.common.turbo.Output;
import io.pixelsdb.pixels.executor.aggregation.FunctionType;
import io.pixelsdb.pixels.executor.join.JoinType;
import io.pixelsdb.pixels.planner.plan.physical.domain.OutputInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.PartialAggregationInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.PartitionInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.PartitionedJoinInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.PartitionedTableInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.ScanTableInfo;
import io.pixelsdb.pixels.planner.plan.physical.input.CombinedPartitionInput;
import io.pixelsdb.pixels.planner.plan.physical.input.PartitionInput;
import io.pixelsdb.pixels.planner.plan.physical.input.PartitionedJoinInput;
import io.pixelsdb.pixels.planner.plan.physical.input.ScanInput;
import io.pixelsdb.pixels.planner.plan.physical.output.JoinOutput;
import io.pixelsdb.pixels.planner.plan.physical.output.PartitionOutput;
import io.pixelsdb.pixels.planner.plan.physical.output.ScanOutput;
import io.pixelsdb.pixels.planner.plan.physical.domain.StorageInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.InputInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.InputSplit;
import io.pixelsdb.pixels.planner.plan.physical.domain.MultiOutputInfo;
import io.pixelsdb.pixels.common.physical.StorageFactory;
public class ScalebilityTest {

    private long cumulativeComputeCostMs;
    private long cumulativeInputCostMs;
    private long cumulativeOutputCostMs;
    private long cumulativeDurationMs;
    private long cumulativeMemoryMB;
    
    private long cumulativeReadBytes;
    private long cumulativeWriteBytes;

    // private String[] order_toread = { "o_orderkey","o_custkey","o_orderdate","o_totalprice"};
    // private boolean[] order_projection = { true, true,true,true};
    // private String[] lineitem_toread = { "l_orderkey", "l_quantity","l_discount","l_linestatus"};
    // private boolean[] lineitem_projection = { true, true,true,true};

    private String[] order_toread = { "o_orderkey","o_custkey","o_orderstatus","o_totalprice","o_orderdate","o_orderpriority"};
    private boolean[] order_projection = {true,true,true,true,true,true};
    private String[] lineitem_toread = { "l_orderkey", "l_partkey","l_suppkey","l_linenumber","l_quantity","l_extendedprice"};
    private boolean[] lineitem_projection = {true,true,true,true,true,true};

    public List<PartitionOutput> FirstStageStarling(HashMap<String, List<InputSplit>> tableToInputSplits,int filePerCf,int numOfPartition) {
            List<CompletableFuture<PartitionOutput>> partitionFuture = new ArrayList<CompletableFuture<PartitionOutput>>();
            List<CompletableFuture<ScanOutput>> futures = new ArrayList<CompletableFuture<ScanOutput>>();
            List<CompletableFuture<Output>> CloudFuture = new ArrayList<CompletableFuture<Output>>();
            List<PartitionOutput> partitionOutputs = new ArrayList<PartitionOutput>();
            List<ScanOutput> fusionOutputs = new ArrayList<ScanOutput>();

        
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
            
            o_tableInfo.setColumnsToRead(order_toread);
            // o_tableInfo.setColumnsToRead(new String[] { "o_orderkey","o_custkey","o_orderdate","o_totalprice"});
            // o_tableInfo.setColumnsToRead(new String[] { "o_orderkey","o_custkey"});
            o_tableInfo.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null, null));
            o_Input.setTableInfo(o_tableInfo);
            o_Input.setProjection(order_projection);
            PartitionInfo o_partitionInfo = new PartitionInfo();
            o_partitionInfo.setNumPartition(numOfPartition);
            o_partitionInfo.setKeyColumnIds(new int[] {0});
            //**********************************************/
            
            /**
             * partition lineitem prepare
             */
            String lineitemFilter = "{\"schemaName\":\"tpch\",\"tableName\":\"lineitem\",\"columnFilters\":{}}";
            PartitionInput l_Input = new PartitionInput();
            l_Input.setTransId(123456);
            ScanTableInfo l_tableInfo = new ScanTableInfo();
            l_tableInfo.setTableName("lineitem");
            int l_NumOfCF = (int)Math.ceil((double)tableToInputSplits.get("lineitem").size()/(double)filePerCf);
            System.out.println("total numof cf for lineitem: " + l_NumOfCF);

            l_tableInfo.setFilter(lineitemFilter);
            l_tableInfo.setBase(true);
            l_tableInfo.setColumnsToRead(lineitem_toread);
            l_tableInfo.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null, null));
            l_Input.setTableInfo(l_tableInfo);
            l_Input.setProjection(lineitem_projection);
            PartitionInfo l_partitionInfo = new PartitionInfo();
            l_partitionInfo.setNumPartition(numOfPartition);
            l_partitionInfo.setKeyColumnIds(new int[] {0});
            //**********************************************/

            for (int o_num=0;o_num<o_NumOfCF;o_num++){
                    o_tableInfo.setInputSplits(tableToInputSplits.get("orders").subList(o_num*filePerCf, (o_num+1)*filePerCf > tableToInputSplits.get("orders").size() ? tableToInputSplits.get("orders").size() : (o_num+1)*filePerCf));
                    o_Input.setPartitionInfo(o_partitionInfo);
                    o_Input.setOutput(new OutputInfo("jingrong-lambda-test/unit_tests/intermediate_result/orders_partition/Part_" + o_num,
                    new StorageInfo(Storage.Scheme.s3, null, null, null, null), false));
                    CloudFuture.add(InvokerCloud.invokeCloudPartition(InvokerFactory.Instance(),o_Input));

            }

            for (int l_num=0;l_num<l_NumOfCF;l_num++){
                    l_tableInfo.setInputSplits(tableToInputSplits.get("lineitem").subList(l_num*filePerCf, (l_num+1)*filePerCf > tableToInputSplits.get("lineitem").size() ? tableToInputSplits.get("lineitem").size() : (l_num+1)*filePerCf));
                    l_Input.setPartitionInfo(l_partitionInfo);
                    l_Input.setOutput(new OutputInfo("jingrong-lambda-test/unit_tests/intermediate_result/lineitem_partition/Part_" + l_num,
                    new StorageInfo(Storage.Scheme.s3, null, null, null, null), false));
                    CloudFuture.add(InvokerCloud.invokeCloudPartition(InvokerFactory.Instance(),l_Input));
            } 

            for (int i=0;i<CloudFuture.size();i++){
                try{
                    Output result = CloudFuture.get(i).get();

                    cumulativeComputeCostMs += result.getCumulativeComputeCostMs();
                    cumulativeInputCostMs += result.getCumulativeInputCostMs();
                    cumulativeOutputCostMs += result.getCumulativeOutputCostMs();
                    cumulativeDurationMs += result.getDurationMs();

                    cumulativeMemoryMB += result.getMemoryMB();
                    cumulativeReadBytes += result.getTotalReadBytes();
                    cumulativeWriteBytes += result.getTotalWriteBytes();
                    
                    partitionOutputs.add((PartitionOutput)result);
                }catch(Exception e){
                    System.out.println("Exception: " + e);
                }
            }

            return partitionOutputs;

    }


    @Test
    public void starlingScalebilityTest(){

        // Common.removeFile("s3://jingrong-lambda-test/unit_tests/intermediate_result/");

        System.setProperty("PIXELS_HOME", "/home/ubuntu/opt/pixels");
        String pixelsHome = System.getenv("PIXELS_HOME");
        
        long startTime = System.nanoTime();
        // Chose local or cloud
        // int functionUSED = 0;
        int filePerCf = 15;
        int numOfPartition = 74;
        int num_joinCF = 37;

        //prepraing the input
        HashMap<String, List<InputSplit>> tableToInputSplits = Common.getTableToInputSplits(Arrays.asList("lineitem","orders"));
        
        //first stage
        List<PartitionOutput> partitionOutputs = FirstStageStarling(tableToInputSplits,filePerCf,numOfPartition);
        

        //second stage
        List<String> orders_partition = new ArrayList<String>();
        List<String> lineitem_partition = new ArrayList<String>();

        for(PartitionOutput out : partitionOutputs){
            if(out.getPath().contains("orders_partition")){
                    orders_partition.add(out.getPath());
                    // System.out.println("customer_partition path: "+out.getPath());
            }else if(out.getPath().contains("lineitem_partition")){
                    lineitem_partition.add(out.getPath());
                    // System.out.println("orders_partition path: "+out.getPath());
            }
        }
        HashMap<String, List<String>> stage2Files = new HashMap<String, List<String>>();
        stage2Files.put("orders_partition", orders_partition);
        stage2Files.put("lineitem_partition", lineitem_partition);

        List<Integer> testHashvalues = new ArrayList<Integer>(numOfPartition);
        for(int i=0;i<numOfPartition;i++){
                testHashvalues.add(i);
        }

        boolean stage2 = SecondStage(stage2Files,testHashvalues,numOfPartition,num_joinCF);

        if(stage2){
            System.out.println("Stage2 success!");

            System.out.println("cumulativeComputeCostMs: " + cumulativeComputeCostMs);
            System.out.println("cumulativeInputCostMs: " + cumulativeInputCostMs);
            System.out.println("cumulativeOutputCostMs: " + cumulativeOutputCostMs);
            System.out.println("cumulativeDurationMs: " + cumulativeDurationMs);
            System.out.println("cumulativeMemoryMB: " + cumulativeMemoryMB);
            System.out.println("cumulativeReadBytes: " + cumulativeReadBytes);
            System.out.println("cumulativeWriteBytes: " + cumulativeWriteBytes);
        }

        System.out.println("Total time: " + (System.nanoTime() - startTime)/1000000 + "ms");



    }

    public boolean SecondStage(HashMap<String, List<String>> stage2Files, List<Integer> hashvalue ,int numOfPartition,int num_joinCF){

        List<CompletableFuture<Output>> futures = new ArrayList<CompletableFuture<Output>>();
        List<JoinOutput> joinOutput = new ArrayList<JoinOutput>();

        int hash_per_cf = (int)Math.ceil((double)hashvalue.size()/(double)num_joinCF);

        PartitionedJoinInput joinInput = new PartitionedJoinInput();
        joinInput.setTransId(123456);

        PartitionedTableInfo leftTableInfo = new PartitionedTableInfo();
        leftTableInfo.setTableName("orders");
        leftTableInfo.setColumnsToRead(order_toread);
        // leftTableInfo.setColumnsToRead(new String[]{"o_orderkey","o_custkey"});
        leftTableInfo.setKeyColumnIds(new int[]{0});
        leftTableInfo.setInputFiles(stage2Files.get("orders_partition"));
        leftTableInfo.setParallelism(8);
        leftTableInfo.setBase(false);
        leftTableInfo.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null, null));
        joinInput.setSmallTable(leftTableInfo);

        PartitionedTableInfo rightTableInfo = new PartitionedTableInfo();
        rightTableInfo.setTableName("lineitem");
        rightTableInfo.setColumnsToRead(lineitem_toread);
        // rightTableInfo.setColumnsToRead(new String[]{ "l_orderkey", "l_quantity"});
        rightTableInfo.setKeyColumnIds(new int[]{0});
        rightTableInfo.setInputFiles(stage2Files.get("lineitem_partition"));
        rightTableInfo.setParallelism(8);
        rightTableInfo.setBase(false);
        rightTableInfo.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null, null));
        joinInput.setLargeTable(rightTableInfo);

        PartitionedJoinInfo joinInfo = new PartitionedJoinInfo();
        joinInfo.setJoinType(JoinType.EQUI_INNER);
        joinInfo.setNumPartition(numOfPartition);

        joinInfo.setSmallColumnAlias(order_toread);
        joinInfo.setLargeColumnAlias(lineitem_toread);
        joinInfo.setSmallProjection(order_projection);
        joinInfo.setLargeProjection(lineitem_projection);

        joinInfo.setPostPartition(false);
        joinInput.setJoinInfo(joinInfo);


        // InvokerFactory factory = InvokerFactory.Instance();
        for(int i=0;i<num_joinCF;i++){
            joinInfo.setHashValues(new ArrayList<Integer>(hashvalue.subList(i*hash_per_cf, (i+1)*hash_per_cf > hashvalue.size() ? hashvalue.size() : (i+1)*hash_per_cf)));
            joinInput.setOutput(new MultiOutputInfo("jingrong-lambda-test/unit_tests/intermediate_result/lineitem_order_join/",
                        new StorageInfo(Storage.Scheme.s3, null, null, null, null),
                        false, Arrays.asList("part_"+i))); // force one file currently
            
            futures.add(InvokerCloud.invokeCloudPartitionJoin(InvokerFactory.Instance(), joinInput));
        }

        for (int i=0;i<futures.size();i++){
            try{
                Output result = futures.get(i).get();

                cumulativeComputeCostMs += result.getCumulativeComputeCostMs();
                cumulativeInputCostMs += result.getCumulativeInputCostMs();
                cumulativeOutputCostMs += result.getCumulativeOutputCostMs();
                cumulativeDurationMs += result.getDurationMs();

                cumulativeMemoryMB += result.getMemoryMB();
                cumulativeReadBytes += result.getTotalReadBytes();
                cumulativeWriteBytes += result.getTotalWriteBytes();
                
                joinOutput.add((JoinOutput)result);
            }catch(Exception e){
                System.out.println("Exception: " + e);
                return false;
            }
        }
        return true;

    }

    @Test
    public void lassv1(){

        List<CompletableFuture<Output>> CloudFuture = new ArrayList<CompletableFuture<Output>>();
        List<PartitionOutput> partitionOutputs = new ArrayList<PartitionOutput>();
        System.setProperty("PIXELS_HOME", "/home/ubuntu/opt/pixels");
        String pixelsHome = System.getenv("PIXELS_HOME");
        
        long startTime = System.nanoTime();
        // Chose local or cloud
        // int functionUSED = 0;
        int numOfPartition = 60;
        int num_joinCF =60;
        int filePerCf = 15;


        String storagepath = "s3://jingrong-lambda-test/unit_tests/intermediate_result/lineitem_partition/";
        // System.out.println("storagepath: " + storagepath);
        try{

        Storage storage = StorageFactory.Instance().getStorage(storagepath);

        long begin = System.currentTimeMillis();
        // List<String> filePath = storage.listPaths(storagepath);
        boolean test = storage.exists("s3://jingrong-lambda-test/unit_tests/intermediate_result/lineitem_partition/Part_0");
        long end = System.currentTimeMillis();   
        System.out.println("listPaths time: " + (end - begin) + "ms"); 
        System.out.println("test");

        }catch(Exception e){
            System.out.println("Exception: " + e);
        }
        //prepraing the input
        HashMap<String, List<InputSplit>> tableToInputSplits = Common.getTableToInputSplits(Arrays.asList("orders","lineitem"));
        

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
        o_tableInfo.setColumnsToRead(new String[] { "o_orderkey","o_custkey"});
        o_tableInfo.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null, null));
        o_Input.setTableInfo(o_tableInfo);
        o_Input.setProjection(new boolean[] { true, true });
        PartitionInfo o_partitionInfo = new PartitionInfo();
        o_partitionInfo.setNumPartition(numOfPartition);
        o_partitionInfo.setKeyColumnIds(new int[] {0});

        for (int o_num=0;o_num<o_NumOfCF;o_num++){
            o_tableInfo.setInputSplits(tableToInputSplits.get("orders").subList(o_num*filePerCf, (o_num+1)*filePerCf > tableToInputSplits.get("orders").size() ? tableToInputSplits.get("orders").size() : (o_num+1)*filePerCf));
            o_Input.setPartitionInfo(o_partitionInfo);
            o_Input.setOutput(new OutputInfo("jingrong-lambda-test/unit_tests/intermediate_result/orders_partition/Part_" + o_num,
            new StorageInfo(Storage.Scheme.s3, null, null, null, null), false));
            CloudFuture.add(InvokerCloud.invokeCloudPartition(InvokerFactory.Instance(),o_Input));
        }


        CombinedPartitionInput input2 = new CombinedPartitionInput();
        input2.setLassversion(1);
        String lineitemFilter = "{\"schemaName\":\"tpch\",\"tableName\":\"lineitem\",\"columnFilters\":{}}";
        input2.setTransId(123456);
        ScanTableInfo tableInfo2 = new ScanTableInfo();
        tableInfo2.setTableName("lineitem");
        tableInfo2.setFilter(lineitemFilter);
        tableInfo2.setBase(true);
        tableInfo2.setColumnsToRead(new String[] { "l_orderkey", "l_quantity"});
        tableInfo2.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null, null));
        
        input2.setProjection(new boolean[] { true, true });
        
        PartitionInfo partitionInfo2 = new PartitionInfo();
        partitionInfo2.setNumPartition(numOfPartition);
        partitionInfo2.setKeyColumnIds(new int[] {0});
        
        input2.setPartitionInfo(partitionInfo2);

        
        //join test
        PartitionedTableInfo leftTableInfo = new PartitionedTableInfo();
        leftTableInfo.setTableName("orders");
        // leftTableInfo.setColumnsToRead(new String[]{"o_orderkey","o_custkey","o_orderdate","o_totalprice"});
        leftTableInfo.setColumnsToRead(new String[]{"o_orderkey","o_custkey"});
        leftTableInfo.setKeyColumnIds(new int[]{0});

        List<String> inputFiles = new ArrayList<String>();
        for (int o_num=0;o_num<o_NumOfCF;o_num++){
            inputFiles.add("s3://jingrong-lambda-test/unit_tests/intermediate_result/orders_partition/Part_" + o_num);
        }

        leftTableInfo.setInputFiles(inputFiles);
        leftTableInfo.setParallelism(8);
        leftTableInfo.setBase(true);
        leftTableInfo.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null, null));
        input2.setSmallTable(leftTableInfo);

        PartitionedTableInfo rightTableInfo = new PartitionedTableInfo();
        rightTableInfo.setTableName("lineitem");
        // rightTableInfo.setColumnsToRead(new String[]{"l_orderkey", "l_quantity","l_discount","l_linestatus"});
        rightTableInfo.setColumnsToRead(new String[]{"l_orderkey", "l_quantity"});
        rightTableInfo.setKeyColumnIds(new int[]{0});

        // rightTableInfo.setInputFiles(inputFiles);
        rightTableInfo.setParallelism(8);
        rightTableInfo.setBase(true);
        rightTableInfo.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null, null));
        
        List<String> largfiles = new ArrayList<String>();
        for (int l_num=0;l_num<num_joinCF;l_num++){
            largfiles.add("s3://jingrong-lambda-test/unit_tests/intermediate_result/lineitem_partition/Part_" + l_num);
        }
        rightTableInfo.setInputFiles(largfiles);
        input2.setLargeTable(rightTableInfo);

        PartitionedJoinInfo joinInfo = new PartitionedJoinInfo();
        joinInfo.setJoinType(JoinType.EQUI_INNER);
        joinInfo.setNumPartition(numOfPartition);
     

        joinInfo.setSmallColumnAlias(order_toread);
        joinInfo.setLargeColumnAlias(lineitem_toread);
        joinInfo.setSmallProjection(order_projection);
        joinInfo.setLargeProjection(lineitem_projection);


        joinInfo.setPostPartition(false);
        // joinInfo.setPostPartitionInfo(new PartitionInfo(new int[] {2}, 20));
        

        input2.setIntermideatePath("s3://jingrong-lambda-test/unit_tests/intermediate_result/");
        input2.setBigTableName("lineitem");
        input2.setSmallTableName("orders");
        input2.setSmallTablePartitionCount(o_NumOfCF);
        input2.setBigTablePartitionCount(num_joinCF);



        int l_file_per_cf = tableToInputSplits.get("lineitem").size()/num_joinCF;
        // int remainder = tableToInputSplits.get("lineitem").size() % num_joinCF;
        System.out.println("total numof cf for lineitem: " + num_joinCF);

        for (int l_num=0;l_num<num_joinCF;l_num++){

            input2.setOutput(new OutputInfo("jingrong-lambda-test/unit_tests/intermediate_result/lineitem_partition/Part_"+l_num,
            new StorageInfo(Storage.Scheme.s3, null, null, null, null), false));
            int startIdx = l_num * l_file_per_cf;
            int endIdx = Math.min((l_num + 1) * l_file_per_cf, tableToInputSplits.get("lineitem").size());

            tableInfo2.setInputSplits(tableToInputSplits.get("lineitem").subList(startIdx, endIdx));
            input2.setTableInfo(tableInfo2);
            input2.setMultiOutput(new MultiOutputInfo("jingrong-lambda-test/unit_tests/intermediate_result/customer_orders_partitioned/",
            new StorageInfo(Storage.Scheme.s3, null, null, null, null),
            false, Arrays.asList("partitioned_join_orders_lineitem"+l_num)));

            int hash_per_cf = (int)Math.ceil((double)numOfPartition/(double)num_joinCF);
            List<Integer> hashValues = new ArrayList<Integer>();
            for (int i = 0; i < hash_per_cf; i++) {
                hashValues.add(l_num*hash_per_cf+i);
            }
            joinInfo.setHashValues(hashValues);
            input2.setJoinInfo(joinInfo);

            // System.out.println("json: " + JSON.toJSONString(input2));


            CloudFuture.add(InvokerCloud.invokeCloudCombinedPartition(InvokerFactory.Instance(),input2));
        } 

        for (int i=0;i<CloudFuture.size();i++){
            try{
                Output result = CloudFuture.get(i).get();

                cumulativeComputeCostMs += result.getCumulativeComputeCostMs();
                cumulativeInputCostMs += result.getCumulativeInputCostMs();
                cumulativeOutputCostMs += result.getCumulativeOutputCostMs();
                cumulativeDurationMs += result.getDurationMs();

                cumulativeMemoryMB += result.getMemoryMB();
                cumulativeReadBytes += result.getTotalReadBytes();
                cumulativeWriteBytes += result.getTotalWriteBytes();
                
                // partitionOutputs.add((PartitionOutput)result);
            }catch(Exception e){
                System.out.println("Exception: " + e);
            }
        }

        System.out.println("cumulativeComputeCostMs: " + cumulativeComputeCostMs);
        System.out.println("cumulativeInputCostMs: " + cumulativeInputCostMs);
        System.out.println("cumulativeOutputCostMs: " + cumulativeOutputCostMs);
        System.out.println("cumulativeDurationMs: " + cumulativeDurationMs);
        System.out.println("cumulativeMemoryMB: " + cumulativeMemoryMB);
        System.out.println("cumulativeReadBytes: " + cumulativeReadBytes);
        System.out.println("cumulativeWriteBytes: " + cumulativeWriteBytes);


        System.out.println("Total time: " + (System.nanoTime() - startTime)/1000000 + "ms");


    }


    @Test
    public void lassvertical(){

        // aws s3 rm s3://jingrong-lambda-test/unit_tests/intermediate_result/ --recursive

        List<CompletableFuture<Output>> CloudFuture = new ArrayList<CompletableFuture<Output>>();
        List<PartitionOutput> partitionOutputs = new ArrayList<PartitionOutput>();
        System.setProperty("PIXELS_HOME", "/home/ubuntu/opt/pixels");
        String pixelsHome = System.getenv("PIXELS_HOME");
        
        long startTime = System.nanoTime();
        int numOfPartition = 30;
        int num_joinCF = 30;
        int filePerCf = 15;
        int lassversion = 2;
        System.out.println("testing lassversion :"+ lassversion);
        //prepraing the input
        HashMap<String, List<InputSplit>> tableToInputSplits = Common.getTableToInputSplits(Arrays.asList("lineitem","orders"));
        

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
        o_tableInfo.setColumnsToRead(order_toread);
        // o_tableInfo.setColumnsToRead(new String[] { "o_orderkey","o_custkey"});
        o_tableInfo.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null, null));
        o_Input.setTableInfo(o_tableInfo);
        o_Input.setProjection(order_projection);
        PartitionInfo o_partitionInfo = new PartitionInfo();
        o_partitionInfo.setNumPartition(numOfPartition);
        o_partitionInfo.setKeyColumnIds(new int[] {0});

        for (int o_num=0;o_num<o_NumOfCF;o_num++){
            o_tableInfo.setInputSplits(tableToInputSplits.get("orders").subList(o_num*filePerCf, (o_num+1)*filePerCf > tableToInputSplits.get("orders").size() ? tableToInputSplits.get("orders").size() : (o_num+1)*filePerCf));
            o_Input.setPartitionInfo(o_partitionInfo);
            o_Input.setOutput(new OutputInfo("jingrong-lambda-test/unit_tests/intermediate_result/orders_partition/Part_" + o_num,
            new StorageInfo(Storage.Scheme.s3, null, null, null, null), false));
            CloudFuture.add(InvokerCloud.invokeCloudPartition(InvokerFactory.Instance(),o_Input));
        }


         /**
         * partition lineitem prepare
         */
        String lineitemFilter = "{\"schemaName\":\"tpch\",\"tableName\":\"lineitem\",\"columnFilters\":{}}";
        PartitionInput l_Input = new PartitionInput();
        l_Input.setTransId(123456);
        ScanTableInfo l_tableInfo = new ScanTableInfo();
        l_tableInfo.setTableName("lineitem");
        
        int l_NumOfCF = (int)Math.ceil((double)tableToInputSplits.get("lineitem").size()/(double)filePerCf);
        System.out.println("total numof cf for lineitem: " + l_NumOfCF);

        int partitioCF = l_NumOfCF;
        int normal_partitioCF = partitioCF - num_joinCF;
        System.out.println("normal_partitioCF: " + normal_partitioCF);


        l_tableInfo.setFilter(lineitemFilter);
        l_tableInfo.setBase(true);
        l_tableInfo.setColumnsToRead(lineitem_toread);
        // l_tableInfo.setColumnsToRead(new String[] { "l_orderkey", "l_quantity"});
        l_tableInfo.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null, null));
        l_Input.setTableInfo(l_tableInfo);
        l_Input.setProjection(lineitem_projection);
        PartitionInfo l_partitionInfo = new PartitionInfo();
        l_partitionInfo.setNumPartition(numOfPartition);
        l_partitionInfo.setKeyColumnIds(new int[] {0});
        //**********************************************/
        for (int l_num=0;l_num<normal_partitioCF;l_num++){
                    l_tableInfo.setInputSplits(tableToInputSplits.get("lineitem").subList(l_num*filePerCf, (l_num+1)*filePerCf > tableToInputSplits.get("lineitem").size() ? tableToInputSplits.get("lineitem").size() : (l_num+1)*filePerCf));
                    l_Input.setPartitionInfo(l_partitionInfo);
                    l_Input.setOutput(new OutputInfo("jingrong-lambda-test/unit_tests/intermediate_result/lineitem_partition/Part_" + (l_num + num_joinCF),
                    new StorageInfo(Storage.Scheme.s3, null, null, null, null), false));
                    CloudFuture.add(InvokerCloud.invokeCloudPartition(InvokerFactory.Instance(),l_Input));
        } 
        
        

        CombinedPartitionInput input2 = new CombinedPartitionInput();
        input2.setLassversion(lassversion);
        // String lineitemFilter = "{\"schemaName\":\"tpch\",\"tableName\":\"lineitem\",\"columnFilters\":{}}";
        input2.setTransId(123456);
        ScanTableInfo tableInfo2 = new ScanTableInfo();
        tableInfo2.setTableName("lineitem");
        tableInfo2.setFilter(lineitemFilter);
        tableInfo2.setBase(true);
        tableInfo2.setColumnsToRead(lineitem_toread);
        // tableInfo2.setColumnsToRead(new String[] { "l_orderkey", "l_quantity"});
        tableInfo2.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null, null));
        
        input2.setProjection(lineitem_projection);
        
        PartitionInfo partitionInfo2 = new PartitionInfo();
        partitionInfo2.setNumPartition(numOfPartition);
        partitionInfo2.setKeyColumnIds(new int[] {0});
        
        input2.setPartitionInfo(partitionInfo2);

        
        //join test
        PartitionedTableInfo leftTableInfo = new PartitionedTableInfo();
        leftTableInfo.setTableName("orders");
        leftTableInfo.setColumnsToRead(order_toread);
        // leftTableInfo.setColumnsToRead(new String[]{"o_orderkey","o_custkey"});
        leftTableInfo.setKeyColumnIds(new int[]{0});

        List<String> inputFiles = new ArrayList<String>();
        for (int o_num=0;o_num<o_NumOfCF;o_num++){
            inputFiles.add("s3://jingrong-lambda-test/unit_tests/intermediate_result/orders_partition/Part_" + o_num);
        }

        leftTableInfo.setInputFiles(inputFiles);
        leftTableInfo.setParallelism(8);
        leftTableInfo.setBase(true);
        leftTableInfo.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null, null));
        input2.setSmallTable(leftTableInfo);

        PartitionedTableInfo rightTableInfo = new PartitionedTableInfo();
        rightTableInfo.setTableName("lineitem");
        rightTableInfo.setColumnsToRead(lineitem_toread);
        // rightTableInfo.setColumnsToRead(new String[]{"l_orderkey", "l_quantity"});
        rightTableInfo.setKeyColumnIds(new int[]{0});

        // rightTableInfo.setInputFiles(inputFiles);
        rightTableInfo.setParallelism(8);
        rightTableInfo.setBase(true);
        rightTableInfo.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null, null));
        
        List<String> largfiles = new ArrayList<String>();
        for (int l_num=0;l_num<partitioCF;l_num++){
            largfiles.add("s3://jingrong-lambda-test/unit_tests/intermediate_result/lineitem_partition/Part_" + l_num);
        }
        rightTableInfo.setInputFiles(largfiles);
        input2.setLargeTable(rightTableInfo);

        


        PartitionedJoinInfo joinInfo = new PartitionedJoinInfo();
        joinInfo.setJoinType(JoinType.EQUI_INNER);
        joinInfo.setNumPartition(numOfPartition);
     
        // joinInfo.setHashValues(new ArrayList<Integer>(hashValues));
        joinInfo.setSmallColumnAlias(order_toread);
        joinInfo.setLargeColumnAlias(lineitem_toread);
        joinInfo.setSmallProjection(order_projection);
        joinInfo.setLargeProjection(lineitem_projection);

        joinInfo.setPostPartition(false);
        // joinInfo.setPostPartitionInfo(new PartitionInfo(new int[] {2}, 20));
        

        input2.setIntermideatePath("s3://jingrong-lambda-test/unit_tests/intermediate_result/");
        input2.setBigTableName("lineitem");
        input2.setSmallTableName("orders");
        input2.setSmallTablePartitionCount(o_NumOfCF);
        input2.setBigTablePartitionCount(partitioCF);
        input2.setMaximumPreLoadFileCount(10);

        System.out.println("total join cf for lineitem: " + num_joinCF);
        for (int l_num=normal_partitioCF;l_num<partitioCF;l_num++){
            
            input2.setOutput(new OutputInfo("jingrong-lambda-test/unit_tests/intermediate_result/lineitem_partition/Part_"+(l_num-normal_partitioCF),
            new StorageInfo(Storage.Scheme.s3, null, null, null, null), false));
            // int startIdx = l_num * l_file_per_cf;
            // int endIdx = Math.min((l_num + 1) * l_file_per_cf, tableToInputSplits.get("lineitem").size());

            tableInfo2.setInputSplits(tableToInputSplits.get("lineitem").subList(l_num*filePerCf, (l_num+1)*filePerCf > tableToInputSplits.get("lineitem").size() ? tableToInputSplits.get("lineitem").size() : (l_num+1)*filePerCf));
            input2.setTableInfo(tableInfo2);
            input2.setMultiOutput(new MultiOutputInfo("jingrong-lambda-test/unit_tests/intermediate_result/customer_orders_partitioned/",
            new StorageInfo(Storage.Scheme.s3, null, null, null, null),
            false, Arrays.asList("partitioned_join_orders_lineitem"+(l_num-normal_partitioCF))));

            // int hash_per_cf = (int)Math.ceil((double)numOfPartition/(double)num_joinCF);
            int hash_per_cf = (int)Math.ceil((double)numOfPartition/2/(double)num_joinCF);
            List<Integer> hashValues = new ArrayList<Integer>();
            for (int i = 0; i < hash_per_cf; i++) {
                hashValues.add((l_num-normal_partitioCF) * hash_per_cf+i);
            }
            joinInfo.setHashValues(hashValues);
            input2.setJoinInfo(joinInfo);

            // System.out.println("json: " + JSON.toJSONString(input2));


            CloudFuture.add(InvokerCloud.invokeCloudCombinedPartition(InvokerFactory.Instance(),input2));
        } 

        for (int i=0;i<CloudFuture.size();i++){
            try{
                Output result = CloudFuture.get(i).get();

                cumulativeComputeCostMs += result.getCumulativeComputeCostMs();
                cumulativeInputCostMs += result.getCumulativeInputCostMs();
                cumulativeOutputCostMs += result.getCumulativeOutputCostMs();
                cumulativeDurationMs += result.getDurationMs();

                cumulativeMemoryMB += result.getMemoryMB();
                cumulativeReadBytes += result.getTotalReadBytes();
                cumulativeWriteBytes += result.getTotalWriteBytes();
                
                // partitionOutputs.add((PartitionOutput)result);
            }catch(Exception e){
                System.out.println("Exception: " + e);
            }
        }

        System.out.println("cumulativeComputeCostMs: " + cumulativeComputeCostMs);
        System.out.println("cumulativeInputCostMs: " + cumulativeInputCostMs);
        System.out.println("cumulativeOutputCostMs: " + cumulativeOutputCostMs);
        System.out.println("cumulativeDurationMs: " + cumulativeDurationMs);
        System.out.println("cumulativeMemoryMB: " + cumulativeMemoryMB);
        System.out.println("cumulativeReadBytes: " + cumulativeReadBytes);
        System.out.println("cumulativeWriteBytes: " + cumulativeWriteBytes);


        System.out.println("Total time: " + (System.nanoTime() - startTime)/1000000 + "ms");


    




    }

}

