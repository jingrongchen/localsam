package helloworld;

import org.junit.Test;

import io.pixelsdb.pixels.common.turbo.InvokerFactory;
import io.pixelsdb.pixels.common.turbo.WorkerType;

public class CFTest {
        @Test
        public void testGetCF() {
                int memorySize = InvokerFactory.Instance().getInvoker(WorkerType.PARTITION).getMemoryMB();
                System.out.println(memorySize);

        }
}
