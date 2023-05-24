package helloworld;
import org.junit.Test;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.planner.plan.physical.domain.*;
import io.pixelsdb.pixels.planner.plan.physical.input.ThreadScanInput;
import io.pixelsdb.pixels.planner.plan.physical.input.ScanInput;
import java.util.Arrays;
import java.util.ArrayList;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import com.amazonaws.services.lambda.runtime.Context;
import io.pixelsdb.pixels.worker.common.BaseThreadScanWorker;
import io.pixelsdb.pixels.worker.common.WorkerContext;
import io.pixelsdb.pixels.worker.common.WorkerContext;
import org.slf4j.Logger;
import io.pixelsdb.pixels.worker.common.WorkerMetrics;
import org.slf4j.LoggerFactory;
import org.apache.logging.log4j.LogManager;
import io.pixelsdb.pixels.worker.common.BaseScanWorker;
import com.alibaba.fastjson.JSON;
import io.pixelsdb.pixels.executor.aggregation.FunctionType;
import java.util.HashMap;

public class AppTest {

  @Test
  public void ThreadAggregationFilter(){
    String filter1= "{\"schemaName\":\"tpch\",\"tableName\":\"orders\"," +
    "\"columnFilters\":{3:{\"columnName\":\"o_orderpriority\"," +
    "\"columnType\":\"CHAR\",\"filterJson\":\"{\\\"javaType\\\":" +
    "\\\"java.lang.String\\\",\\\"isAll\\\":false,\\\"isNone\\\":false," +
    "\\\"allowNull\\\":false,\\\"onlyNull\\\":false,\\\"ranges\\\":[]," +
    "\\\"discreteValues\\\":[{\\\"type\\\":\\\"INCLUDED\\\"," +
    "\\\"value\\\":\\\"3-MEDIUM\\\"}]}\"}}}";

    String filter2= "{\"schemaName\":\"tpch\",\"tableName\":\"orders\"," +
    "\"columnFilters\":{2:{\"columnName\":\"o_orderstatus\"," +
    "\"columnType\":\"CHAR\",\"filterJson\":\"{\\\"javaType\\\":" +
    "\\\"java.lang.String\\\",\\\"isAll\\\":false,\\\"isNone\\\":false," +
    "\\\"allowNull\\\":false,\\\"onlyNull\\\":false,\\\"ranges\\\":[]," +
    "\\\"discreteValues\\\":[{\\\"type\\\":\\\"INCLUDED\\\"," +
    "\\\"value\\\":\\\"P\\\"}]}\"}}}";
    
    List<String> filterlist=Arrays.asList(filter1,filter2);

    ThreadScanInput scaninput = new ThreadScanInput();
    scaninput.setQueryId(123456);
    ThreadScanTableInfo tableInfo = new ThreadScanTableInfo();
    tableInfo.setTableName("orders");

    List<InputSplit> myList = new ArrayList<InputSplit>();
    try {
            List<String> allLines = Files.readAllLines(Paths.get("/home/ubuntu/opt/pixels/pixels-experiments/orders-url-small.txt"));

            for (String line : allLines) {
                    InputSplit temp= new InputSplit(Arrays.asList(new InputInfo(line, 0, -1)));
                    myList.add(temp);
            }
    } catch (IOException e) {
            e.printStackTrace();
    }
    tableInfo.setInputSplits(myList);
    
    tableInfo.setColumnsToRead(new String[]{"o_orderkey", "o_custkey", "o_orderstatus", "o_orderpriority","o_totalprice"});
    tableInfo.setFilter(filterlist);
    tableInfo.setBase(true);
    tableInfo.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null));
    scaninput.setTableInfo(tableInfo);
    scaninput.setScanProjection(new boolean[]{true, true, true, true, true});
    scaninput.setPartialAggregationPresent(true);

    List<PartialAggregationInfo> aggregationlist = new ArrayList<>();
    // aggregation1
    PartialAggregationInfo aggregationInfo1 = new PartialAggregationInfo();
    aggregationInfo1.setAggregateColumnIds(new int[]{4});
    aggregationInfo1.setFunctionTypes(new FunctionType[]{FunctionType.SUM});
    aggregationInfo1.setGroupKeyColumnAlias(new String[]{"o_custkey_agg"});
    aggregationInfo1.setGroupKeyColumnIds(new int[]{1});
    aggregationInfo1.setNumPartition(0);
    aggregationInfo1.setPartition(false);
    aggregationInfo1.setResultColumnAlias(new String[]{"num_agg"});
    aggregationInfo1.setResultColumnTypes(new String[]{"bigint"});
    // aggregation 2
    PartialAggregationInfo aggregationInfo2 = new PartialAggregationInfo();
    aggregationInfo2.setAggregateColumnIds(new int[]{1});
    aggregationInfo2.setFunctionTypes(new FunctionType[]{FunctionType.COUNT});
    aggregationInfo2.setGroupKeyColumnAlias(new String[]{"o_custkey_agg"});
    aggregationInfo2.setGroupKeyColumnIds(new int[]{1});
    aggregationInfo2.setNumPartition(0);
    aggregationInfo2.setPartition(false);
    aggregationInfo2.setResultColumnAlias(new String[]{"num_agg"});
    aggregationInfo2.setResultColumnTypes(new String[]{"bigint"});
    //
    aggregationlist.add(aggregationInfo1);
    aggregationlist.add(aggregationInfo2);
    scaninput.setPartialAggregationInfo(aggregationlist);
    List<String> list=new ArrayList<String>();
    list.add("s3://jingrong-lambda-test/unit_tests/test_scan1/");
    list.add("s3://jingrong-lambda-test/unit_tests/test_scan2/");

    //randomwfilename shouold be false?????
    ThreadOutputInfo threadoutput = new ThreadOutputInfo(list, false,
      new StorageInfo(Storage.Scheme.s3, null, null, null), true);
    scaninput.setOutput(threadoutput);
    HashMap<String, List<Integer>> filterOnAggreation = new HashMap<String, List<Integer>>();
    filterOnAggreation.put("0", Arrays.asList(0));
    filterOnAggreation.put("1", Arrays.asList(1));
    scaninput.setFilterOnAggreation(filterOnAggreation);

    System.out.println(JSON.toJSONString(scaninput));
    WorkerMetrics workerMetrics = new WorkerMetrics();
    Logger logger = LoggerFactory.getLogger(AppTest.class);
    WorkerContext workerContext = new WorkerContext(logger, workerMetrics, "123456");
    BaseThreadScanWorker baseWorker = new BaseThreadScanWorker(workerContext);
    baseWorker.process(scaninput);
  }

  @Test
  public void BaseResponse(){
    String filter1 =
    "{\"schemaName\":\"tpch\",\"tableName\":\"orders\"," +
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
        List<String> allLines = Files.readAllLines(Paths.get("/home/ubuntu/opt/pixels/pixels-experiments/orders-url-small.txt"));

        for (String line : allLines) {
                InputSplit temp= new InputSplit(Arrays.asList(new InputInfo(line, 0, -1)));
                myList.add(temp);
        }
    } catch (IOException e) {
        e.printStackTrace();
    }

    ScanInput scaninput = new ScanInput();
    scaninput.setQueryId(123456);
    ScanTableInfo tableInfo = new ScanTableInfo();
    tableInfo.setTableName("orders");

    tableInfo.setInputSplits(myList);
    tableInfo.setColumnsToRead(new String[]{"o_orderkey", "o_custkey", "o_orderstatus", "o_orderdate"});
    tableInfo.setFilter(filter1);
    tableInfo.setBase(true);
    tableInfo.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null));
    scaninput.setTableInfo(tableInfo);
    scaninput.setScanProjection(new boolean[]{true, true, true, true});

    scaninput.setOutput(new OutputInfo("s3://jingrong-lambda-test/unit_tests/test_scan1", true,
        new StorageInfo(Storage.Scheme.s3, null, null, null), true));

    System.out.println(JSON.toJSONString(scaninput)); 
    
    WorkerMetrics workerMetrics = new WorkerMetrics();
    Logger logger = LoggerFactory.getLogger(AppTest.class);
    WorkerContext workerContext = new WorkerContext(logger, workerMetrics, "123456");
    BaseScanWorker baseWorker = new BaseScanWorker(workerContext);
    baseWorker.process(scaninput);
  }
  
  
  @Test
  public void successfulResponse() {
//     App app = new App();
    
//     String filter1 =
//                 "{\"schemaName\":\"tpch\",\"tableName\":\"orders\"," +
//                         "\"columnFilters\":{1:{\"columnName\":\"o_custkey\",\"columnType\":\"LONG\"," +
//                         "\"filterJson\":\"{\\\"javaType\\\":\\\"long\\\",\\\"isAll\\\":false," +
//                         "\\\"isNone\\\":false,\\\"allowNull\\\":false,\\\"ranges\\\":[{" +
//                         "\\\"lowerBound\\\":{\\\"type\\\":\\\"UNBOUNDED\\\"}," +
//                         "\\\"upperBound\\\":{\\\"type\\\":\\\"INCLUDED\\\",\\\"value\\\":100}}," +
//                         "{\\\"lowerBound\\\":{\\\"type\\\":\\\"EXCLUDED\\\",\\\"value\\\":200}," +
//                         "\\\"upperBound\\\":{\\\"type\\\":\\\"UNBOUNDED\\\"}}]," +
//                         "\\\"discreteValues\\\":[]}\"}}}";
//     String filter2 =
//             "{\"schemaName\":\"tpch\",\"tableName\":\"orders\"," +
//                     "\"columnFilters\":{1:{\"columnName\":\"o_orderkey\",\"columnType\":\"LONG\"," +
//                     "\"filterJson\":\"{\\\"javaType\\\":\\\"long\\\",\\\"isAll\\\":false," +
//                     "\\\"isNone\\\":false,\\\"allowNull\\\":false,\\\"ranges\\\":[{" +
//                     "\\\"lowerBound\\\":{\\\"type\\\":\\\"UNBOUNDED\\\"}," +
//                     "\\\"upperBound\\\":{\\\"type\\\":\\\"INCLUDED\\\",\\\"value\\\":100}}," +
//                     "{\\\"lowerBound\\\":{\\\"type\\\":\\\"EXCLUDED\\\",\\\"value\\\":200}," +
//                     "\\\"upperBound\\\":{\\\"type\\\":\\\"UNBOUNDED\\\"}}]," +
//                     "\\\"discreteValues\\\":[]}\"}}}";
//     List<String> filterlist=Arrays.asList(filter1,filter2);

//     ThreadScanInput scaninput = new ThreadScanInput();
//     scaninput.setQueryId(123456);
//     ThreadScanTableInfo tableInfo = new ThreadScanTableInfo();
//     tableInfo.setTableName("orders");

//     List<InputSplit> myList = new ArrayList<InputSplit>();
//     try {
//             List<String> allLines = Files.readAllLines(Paths.get("/home/ubuntu/opt/pixels/pixels-experiments/orders-url-small.txt"));

//             for (String line : allLines) {
//                     InputSplit temp= new InputSplit(Arrays.asList(new InputInfo(line, 0, -1)));
//                     myList.add(temp);
//             }
//     } catch (IOException e) {
//             e.printStackTrace();
//     }
//     tableInfo.setInputSplits(myList);
    

//     // tableInfo.setInputSplits(Arrays.asList(
//     //         new InputSplit(Arrays.asList(new InputInfo("jingrong-test/orders/v-0-order/20230425100700_2.pxl", 0, -1)))));

//     tableInfo.setColumnsToRead(new String[]{"o_orderkey", "o_custkey", "o_orderstatus", "o_orderdate"});
//     tableInfo.setFilter(filterlist);
//     tableInfo.setBase(true);
//     tableInfo.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null));
//     scaninput.setTableInfo(tableInfo);
//     scaninput.setScanProjection(new boolean[]{true, true, true, true});
    
//     List<String> list=new ArrayList<String>();
//     list.add("s3://jingrong-lambda-test/unit_tests/test_scan1/");
//     list.add("s3://jingrong-lambda-test/unit_tests/test_scan2/");
//     ThreadOutputInfo threadoutput = new ThreadOutputInfo(list, true,
//       new StorageInfo(Storage.Scheme.s3, null, null, null), true);
    
//     scaninput.setOutput(threadoutput);
//     WorkerMetrics workerMetrics = new WorkerMetrics();


//     Logger logger = LoggerFactory.getLogger(AppTest.class);
//     WorkerContext workerContext = new WorkerContext(logger, workerMetrics, "123456");
//     BaseThreadScanWorker baseWorker = new BaseThreadScanWorker(workerContext);
//     baseWorker.process(scaninput);

  
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
