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
import io.pixelsdb.pixels.worker.common.WorkerMetrics;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.alibaba.fastjson.JSON;
import com.google.common.base.Joiner;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.turbo.InvokerFactory;
import io.pixelsdb.pixels.common.turbo.WorkerType;
import io.pixelsdb.pixels.planner.plan.physical.domain.*;
import io.pixelsdb.pixels.planner.plan.physical.input.PartitionInput;
import io.pixelsdb.pixels.planner.plan.physical.output.PartitionOutput;
import io.pixelsdb.pixels.planner.plan.physical.input.ThreadScanInput;
import io.pixelsdb.pixels.planner.plan.physical.output.ScanOutput;
import io.pixelsdb.pixels.worker.common.BaseThreadScanWorker;

import io.pixelsdb.pixels.planner.plan.physical.input.ScanInput;
import io.pixelsdb.pixels.planner.plan.physical.output.ScanOutput;
import io.pixelsdb.pixels.worker.common.BaseScanWorker;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;


// public class App implements RequestHandler<ScanInput, ScanOutput>
// {
//     private static final Logger logger = LoggerFactory.getLogger(App.class);
//     private final WorkerMetrics workerMetrics = new WorkerMetrics();

//     @Override
//     public ScanOutput handleRequest(ScanInput event, Context context)
//     {
//         WorkerContext workerContext = new WorkerContext(logger, workerMetrics, context.getAwsRequestId());
//         BaseScanWorker baseWorker = new BaseScanWorker(workerContext);
//         return baseWorker.process(event);
//     }
// }

// public class App implements RequestHandler<ThreadScanInput, ScanOutput>
// {
//     private static final Logger logger = LogManager.getLogger(App.class);
//     private final WorkerMetrics workerMetrics = new WorkerMetrics();
    
//     @Override
//     public ScanOutput handleRequest(ThreadScanInput event, Context context)
//     {
//         WorkerContext workerContext = new WorkerContext(logger, workerMetrics, context.getAwsRequestId());
//         BaseThreadScanWorker baseWorker = new BaseThreadScanWorker(workerContext);
//         return baseWorker.process(event);
//     }

// }

import java.io.BufferedWriter;
import java.io.FileWriter;


public class App implements RequestHandler<Object, String> {

    @Override
    public String handleRequest(Object input, Context context) {
        String fileName = "/tmp/myfile.txt";
        
        try {
            // Create a FileWriter with the specified file name
            FileWriter fileWriter = new FileWriter(fileName);

            // Wrap the FileWriter in a BufferedWriter for efficient writing
            BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);

            // Write some content to the file
            bufferedWriter.write("Hello, AWS Lambda!");
            bufferedWriter.newLine();  // Add a new line

            // Close the buffered writer
            bufferedWriter.close();
            System.out.println("File written successfully.");

            // reads the tmp file
            BufferedReader br = new BufferedReader(new InputStreamReader(new URL("file:///tmp/myfile.txt").openStream()));
            String content = br.lines().collect(Collectors.joining(System.lineSeparator()));
            System.out.println(content);
            System.out.println("File read successfully.");

            return "File written successfully.";
        } catch (IOException e) {
            // Handle any errors
            return "Error writing file: " + e.getMessage();
        }
    }
}


