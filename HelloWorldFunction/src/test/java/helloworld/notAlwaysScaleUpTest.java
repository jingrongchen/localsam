package helloworld;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.junit.Test;

import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.turbo.InvokerFactory;
import io.pixelsdb.pixels.common.turbo.Output;
import io.pixelsdb.pixels.executor.join.JoinAlgorithm;
import io.pixelsdb.pixels.executor.join.JoinType;
import io.pixelsdb.pixels.executor.predicate.TableScanFilter;
import io.pixelsdb.pixels.planner.PixelsPlanner;
import io.pixelsdb.pixels.planner.plan.logical.BaseTable;
import io.pixelsdb.pixels.planner.plan.logical.Join;
import io.pixelsdb.pixels.planner.plan.logical.JoinEndian;
import io.pixelsdb.pixels.planner.plan.logical.JoinedTable;
import io.pixelsdb.pixels.planner.plan.physical.domain.InputSplit;
import io.pixelsdb.pixels.planner.plan.physical.domain.OutputInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.PartitionInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.ScanTableInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.StorageInfo;
import io.pixelsdb.pixels.planner.plan.physical.input.PartitionInput;

public class notAlwaysScaleUpTest {

    private String[] order_toread = { "o_orderkey","o_custkey","o_orderstatus","o_totalprice","o_orderdate","o_orderpriority"};
    private boolean[] order_projection = {true,true,true,true,true,true};
    private String[] lineitem_toread = { "l_orderkey", "l_partkey","l_suppkey","l_linenumber","l_quantity","l_extendedprice"};
    private boolean[] lineitem_projection = {true,true,true,true,true,true};


    private long cumulativeComputeCostMs;
    private long cumulativeInputCostMs;
    private long cumulativeOutputCostMs;
    private long cumulativeDurationMs;
    private long cumulativeMemoryMB;
    private long cumulativeReadBytes;
    private long cumulativeWriteBytes;

    @Test
    public void testPartitionScaleUP() {

        List<CompletableFuture<Output>> CloudFuture = new ArrayList<CompletableFuture<Output>>();
        HashMap<String, List<InputSplit>> tableToInputSplits = Common.getTableToInputSplits(Arrays.asList("lineitem","orders"));
        int filePerCf = 5;
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
        o_partitionInfo.setNumPartition(30);
        o_partitionInfo.setKeyColumnIds(new int[] {0});

        //record start time
        long startTime = System.currentTimeMillis();

        for (int o_num=0;o_num<o_NumOfCF;o_num++){
            o_tableInfo.setInputSplits(tableToInputSplits.get("orders").subList(o_num*filePerCf, (o_num+1)*filePerCf > tableToInputSplits.get("orders").size() ? tableToInputSplits.get("orders").size() : (o_num+1)*filePerCf));
            o_Input.setPartitionInfo(o_partitionInfo);
            o_Input.setOutput(new OutputInfo("jingrong-lambda-test/unit_tests/intermediate_result/orders_partition/Part_" + o_num,
            new StorageInfo(Storage.Scheme.s3, null, null, null, null), false));
            CloudFuture.add(InvokerCloud.invokeCloudPartition(InvokerFactory.Instance(),o_Input));
        }

        int totalduration = 0;
        for (int i = 0; i < CloudFuture.size(); i++) {
            try {
                Output out =CloudFuture.get(i).get();
                totalduration += out.getDurationMs();
                cumulativeReadBytes+=out.getTotalReadBytes();
                cumulativeWriteBytes+=out.getTotalWriteBytes();
                cumulativeMemoryMB+=out.getMemoryMB();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        //record end time
        long endTime = System.currentTimeMillis();

        //calculate money spent
        long timeElapsed = endTime - startTime;
        double price = totalduration * 0.0000001500;

        System.out.println("cumulativeReadBytes"+cumulativeReadBytes);
        System.out.println("cumulativeWriteBytes"+cumulativeWriteBytes);
        System.out.println("cumulativeMemoryMB"+cumulativeMemoryMB);

        System.out.println("time elapsed: " + timeElapsed + "ms");
        System.out.println("total duration: " + totalduration + "ms");
        System.out.println("Price: " + price + "USD");
        

    }

    @Test
    public void testPartitionScaleUPLineitem() {

        List<CompletableFuture<Output>> CloudFuture = new ArrayList<CompletableFuture<Output>>();
        HashMap<String, List<InputSplit>> tableToInputSplits = Common.getTableToInputSplits(Arrays.asList("lineitem","orders"));
        int filePerCf = 15;
        /**
         * partition orders prepare
         */
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
        l_partitionInfo.setNumPartition(30);
        l_partitionInfo.setKeyColumnIds(new int[] {0});

        //record start time
        long startTime = System.currentTimeMillis();

        for (int l_num=0;l_num<l_NumOfCF;l_num++){
            l_tableInfo.setInputSplits(tableToInputSplits.get("lineitem").subList(l_num*filePerCf, (l_num+1)*filePerCf > tableToInputSplits.get("lineitem").size() ? tableToInputSplits.get("lineitem").size() : (l_num+1)*filePerCf));
            l_Input.setPartitionInfo(l_partitionInfo);
            l_Input.setOutput(new OutputInfo("jingrong-lambda-test/unit_tests/intermediate_result/lineitem_partition/Part_" + l_num,
            new StorageInfo(Storage.Scheme.s3, null, null, null, null), false));
            CloudFuture.add(InvokerCloud.invokeCloudPartition(InvokerFactory.Instance(),l_Input));
        } 

        int totalduration = 0;
        for (int i = 0; i < CloudFuture.size(); i++) {
            try {
                Output out =CloudFuture.get(i).get();
                totalduration += out.getDurationMs();
                cumulativeReadBytes+=out.getTotalReadBytes();
                cumulativeWriteBytes+=out.getTotalWriteBytes();
                cumulativeMemoryMB+=out.getMemoryMB();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        //record end time
        long endTime = System.currentTimeMillis();

        //calculate money spent
        long timeElapsed = endTime - startTime;
        double price = totalduration * 0.0000000667 ;

        System.out.println("cumulativeReadBytes: "+cumulativeReadBytes);
        System.out.println("cumulativeWriteBytes: "+cumulativeWriteBytes);
        System.out.println("cumulativeMemoryMB: "+cumulativeMemoryMB);

        System.out.println("time elapsed: " + timeElapsed + "ms");
        System.out.println("total duration: " + totalduration + "ms");
        System.out.println("Price: " + price + "USD");
        

    }


}
