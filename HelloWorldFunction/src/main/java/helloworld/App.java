package helloworld;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;

import io.pixelsdb.pixels.worker.common.BasePartitionWorker;
import io.pixelsdb.pixels.worker.common.WorkerContext;
import org.slf4j.Logger;
import io.pixelsdb.pixels.worker.common.WorkerMetrics;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.google.common.base.Joiner;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.turbo.InvokerFactory;
import io.pixelsdb.pixels.common.turbo.WorkerType;
import io.pixelsdb.pixels.planner.plan.physical.domain.*;
import io.pixelsdb.pixels.planner.plan.physical.input.PartitionInput;
import io.pixelsdb.pixels.planner.plan.physical.output.PartitionOutput;
import io.pixelsdb.pixels.planner.plan.physical.input.ScanInput;
import io.pixelsdb.pixels.planner.plan.physical.output.ScanOutput;
import io.pixelsdb.pixels.worker.common.BaseScanWorker;


public class App implements RequestHandler<ScanInput, ScanOutput>
{
    private static final Logger logger = LoggerFactory.getLogger(App.class);
    private final WorkerMetrics workerMetrics = new WorkerMetrics();

    @Override
    public ScanOutput handleRequest(ScanInput event, Context context)
    {
        WorkerContext workerContext = new WorkerContext(logger, workerMetrics, context.getAwsRequestId());
        BaseScanWorker baseWorker = new BaseScanWorker(workerContext);
        return baseWorker.process(event);
    }
}
