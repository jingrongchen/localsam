package helloworld;

import org.junit.Test;
import java.util.Optional;
import io.pixelsdb.pixels.planner.DLASPlanner;
import java.nio.file.Paths;

import io.pixelsdb.pixels.planner.plan.physical.output.PartitionOutput;
import io.pixelsdb.pixels.planner.plan.physical.input.PartitionedJoinInput;
import io.pixelsdb.pixels.planner.plan.physical.domain.PartitionedTableInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.PartitionedJoinInfo;
import io.pixelsdb.pixels.executor.join.JoinType;
import io.pixelsdb.pixels.planner.plan.physical.domain.MultiOutputInfo;
import io.pixelsdb.pixels.worker.common.BasePartitionedJoinWorker;
import com.alibaba.fastjson.JSONArray;



//for get operator 
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ObjectArrays;
import com.google.protobuf.InvalidProtocolBufferException;
import io.pixelsdb.pixels.common.exception.InvalidArgumentException;
import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.layout.*;
import io.pixelsdb.pixels.common.metadata.SchemaTableName;
import io.pixelsdb.pixels.common.metadata.domain.Layout;
import io.pixelsdb.pixels.common.metadata.domain.Ordered;
import io.pixelsdb.pixels.common.metadata.domain.Projections;
import io.pixelsdb.pixels.common.metadata.domain.Splits;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.executor.join.JoinAlgorithm;
import io.pixelsdb.pixels.planner.plan.PlanOptimizer;
import io.pixelsdb.pixels.planner.plan.logical.*;
import io.pixelsdb.pixels.planner.plan.physical.*;
import io.pixelsdb.pixels.planner.plan.physical.domain.*;
import io.pixelsdb.pixels.planner.plan.physical.input.*;
import java.util.Optional;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.alibaba.fastjson.JSON;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.BreakIterator;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;
import com.fasterxml.jackson.databind.node.ObjectNode;
// import com.jayway.jsonpath.internal.function.numeric.Max;

import java.util.ArrayList;
import java.util.HashMap;
import org.jgrapht.Graph;
import org.jgrapht.Graphs;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleGraph;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Stack;

// import DAG
// import com.fasterxml.jackson.databind.util.TokenBuffer;
// import com.google.common.collect.BoundType;
// import com.google.protobuf.ByteString.Output;

import org.jgrapht.graph.DirectedAcyclicGraph;
import org.jgrapht.Graph;
import org.jgrapht.traverse.BreadthFirstIterator;
import org.jgrapht.traverse.DepthFirstIterator;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
// import org.eclipse.jetty.server.Response.OutputType;
import org.jgrapht.graph.DefaultEdge;
import java.util.Arrays;
import java.util.Set;
import java.util.UUID;
import java.util.HashSet;
import org.jgrapht.graph.AsSubgraph;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import io.pixelsdb.pixels.planner.plan.physical.output.ScanOutput;
import java.util.concurrent.FutureTask;

import io.grpc.netty.shaded.io.netty.channel.local.LocalAddress;
import io.pixelsdb.pixels.common.lock.EtcdAutoIncrement;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;

//invoke lambda
import io.pixelsdb.pixels.executor.predicate.TableScanFilter;

import java.util.SortedMap;
import java.util.TreeMap;
import io.pixelsdb.pixels.executor.predicate.ColumnFilter;
import java.util.Map;  
import io.pixelsdb.pixels.executor.predicate.Filter;
import io.pixelsdb.pixels.executor.aggregation.FunctionType;
import io.pixelsdb.pixels.executor.predicate.Bound;

import java.util.concurrent.Future;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.stream.Collectors;
import io.pixelsdb.pixels.common.turbo.Output;
import java.util.Collections;

// import io.pixelsdb.pixels.worker.common.BaseAggregationWorker;
// import io.pixelsdb.pixels.worker.common.BaseJoinScanFusionWorker;
// import io.pixelsdb.pixels.worker.common.BasePartitionWorker;
///for local invoking
// import io.pixelsdb.pixels.worker.common.BaseThreadScanWorker;
// import io.pixelsdb.pixels.worker.common.WorkerContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.units.qual.s;

// import io.pixelsdb.pixels.worker.common.WorkerMetrics;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.Iterator;

import io.pixelsdb.pixels.planner.plan.physical.output.AggregationOutput;
import io.pixelsdb.pixels.planner.plan.physical.output.FusionOutput;
import io.pixelsdb.pixels.planner.plan.physical.output.PartitionOutput;
import io.pixelsdb.pixels.executor.join.JoinType;
// import io.pixelsdb.pixels.worker.common.BasePartitionedJoinWorker;
import com.alibaba.fastjson.JSONArray;
import com.google.common.base.Joiner;
//for get operator 
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ObjectArrays;
import com.google.protobuf.InvalidProtocolBufferException;
import io.pixelsdb.pixels.common.exception.InvalidArgumentException;
import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.layout.*;
import io.pixelsdb.pixels.common.metadata.SchemaTableName;
import io.pixelsdb.pixels.common.metadata.domain.Layout;
import io.pixelsdb.pixels.common.metadata.domain.Ordered;
import io.pixelsdb.pixels.common.metadata.domain.Projections;
import io.pixelsdb.pixels.common.metadata.domain.Splits;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.executor.join.JoinAlgorithm;
import io.pixelsdb.pixels.planner.plan.PlanOptimizer;
import io.pixelsdb.pixels.planner.plan.logical.*;
import io.pixelsdb.pixels.planner.plan.physical.*;
import io.pixelsdb.pixels.planner.plan.physical.domain.*;
//for aggregation
//For join
import io.pixelsdb.pixels.planner.plan.physical.input.*;
import java.util.Optional;
import io.pixelsdb.pixels.planner.plan.logical.calcite.field;

import com.fasterxml.jackson.core.type.TypeReference;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;


public class TestDLASPlanner {
    
    @Test
    public void testDLAS()throws Exception{
        
        DLASPlanner DLASPlanner = new DLASPlanner(Paths.get("/home/ubuntu/opt/pixels/pixels-parser/src/test/java/io/pixelsdb/pixels/parser/logicalplan/TPCHQ18.json")
        ,123456,true,false,Optional.empty());

        DLASPlanner.generateDAG();
        DLASPlanner.genSubGraphs();
        DLASPlanner.genStages();
        System.out.println("Start invoke asd ssda");
        DLASPlanner.invoke();



        // BaseTable orders = new BaseTable(
        //     "tpch", "orders", "orders",
        //     new String[] {"o_orderkey", "o_custkey"},
        //     TableScanFilter.empty("tpch", "orders"));

        // BaseTable lineitem = new BaseTable(
        //         "tpch", "lineitem", "lineitem",
        //         new String[] {"l_orderkey", "l_quantity"},
        //         TableScanFilter.empty("tpch", "lineitem"));

        // Join join1 = new Join(orders, lineitem,
        //         new String[]{"o_orderkey", "o_custkey"},
        //         new String[]{"l_orderkey", "l_quantity"},
        //         new int[]{0}, new int[]{0}, new boolean[]{true, true},
        //         new boolean[]{true, true},
        //         JoinEndian.SMALL_LEFT, JoinType.EQUI_INNER, JoinAlgorithm.PARTITIONED);

        // JoinedTable joinedTable = new JoinedTable("tpch",
        //         "orders_join_lineitem",
        //         "orders_join_lineitem_table", join1);
                
        

        // DLASPlanner joinExecutor = new DLASPlanner(
        //     123456, joinedTable, true, false, Optional.empty());

        // Operator joinOperator = joinExecutor.getRootOperator();


        // List<InputSplit> testsplit=DLASPlanner.getInputSplits(lineitem);
        // System.out.println("testsplit size: "+testsplit.size());
        // System.out.println("infos size: "+testsplit.get(0).getInputInfos().size());

    
    }




}
