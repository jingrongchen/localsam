package helloworld;

import io.pixelsdb.pixels.common.metadata.domain.Schema;
import io.pixelsdb.pixels.core.PixelsProto;
import io.pixelsdb.pixels.core.PixelsProto.Type;
import io.pixelsdb.pixels.core.PixelsFooterCache;
/*
 * Copyright 2018 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.physical.Storage.Scheme;
import io.pixelsdb.pixels.core.PixelsReader;
import io.pixelsdb.pixels.core.PixelsReaderImpl;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import java.io.IOException;
import java.util.List;

import javax.print.event.PrintJobListener;

import io.pixelsdb.pixels.worker.common.WorkerCommon;
import org.junit.Test;
import io.pixelsdb.pixels.planner.plan.physical.domain.StorageInfo;
import io.pixelsdb.pixels.storage.s3.S3;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.planner.plan.physical.domain.StorageInfo;
import io.pixelsdb.pixels.planner.plan.physical.input.ScanInput;
import io.pixelsdb.pixels.planner.plan.physical.domain.StorageInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.*;
/**
 * @author hank
 */
public class ResultTest
{   
    @Test
    public void BaseResponse()
    {
        String currentPath = "s3://jingrong-lambda-test/unit_tests/intermediate_result/testlineitem/Part_0/scan_0";
        try {

            StorageFactory storagefactory = StorageFactory.Instance();
            Storage storage =storagefactory.getStorage(currentPath);
            PixelsReader reader=WorkerCommon.getReader(currentPath,storage);
           
            TypeDescription schema = reader.getFileSchema();
            List<String> fieldNames = schema.getFieldNames();

            System.out.println(fieldNames);
            String[] cols = new String[fieldNames.size()];
            for (int i = 0; i < fieldNames.size(); i++) {
                cols[i] = fieldNames.get(i);
            }

            PixelsReaderOption option = new PixelsReaderOption();
            option.skipCorruptRecords(true);
            option.tolerantSchemaEvolution(true);
            option.includeCols(cols);
            PixelsRecordReader recordReader = reader.read(option);
            System.out.println(reader.getRowGroupStats().size());
            // System.out.println(reader.getRowGroupInfo(0).getNumberOfRows());
            int batchSize = 10;
            VectorizedRowBatch rowBatch;
            int len = 0;
            int num = 0;
            int row = 0;

            for(int i=0;i<100;i++){
                rowBatch = recordReader.readBatch(batchSize);
                // System.out.println(rowBatch);
                row++;
                String result = rowBatch.toString();
                System.out.println(result);
                // len += result.length();
                // System.out.println("loop:" + row + "," + rowBatch.size);
                if (rowBatch.endOfFile) {
                    num += rowBatch.size;
                    break;
                }
                num += rowBatch.size;

            }
            System.out.println(row + "," + num);
            reader.close();
        } catch (IOException e) {
            System.out.println("Err path: " + currentPath.toString());
            e.printStackTrace();
        }
    }

    @Test
    public void justest(){

        System.out.println("test result : "+ 5/3);
    }

    @Test
    public void testMetadata()
    {   
        String path="s3://jingrong-test/tpch/lineitem/v-0-order/20230425092347_48.pxl";
        // String path = "s3://jingrong-lambd a-test/unit_tests/intermediate_result/customer_orders_lineitem_partitionjoin/part_0";
        PixelsReader reader;
        try
        {
            Storage storage = StorageFactory.Instance().getStorage("s3");
            reader = PixelsReaderImpl.newBuilder()
                    .setStorage(storage)
                    .setPath(path)
                    .setPixelsFooterCache(new PixelsFooterCache())
                    .build();

            List<PixelsProto.Type> types = reader.getFooter().getTypesList();
            for (PixelsProto.Type type : types)
            {
                System.out.println(type);
            }
            // System.out.println(reader.getColumnStat("c_custkey"));
            System.out.println("num of rows: "+reader.getNumberOfRows()); 
            System.out.println("num of row group: "+reader.getRowGroupInfos().size());
            System.out.println("num of rows in a row group: "+reader.getRowGroupInfos().get(0).getNumberOfRows());
            System.out.println("row group hash? : "+reader.getRowGroupInfos().get(0).getPartitionInfo().getHashValue());
            System.out.println("data length: "+reader.getRowGroupInfos().get(0).getDataLength());
            // System.out.println(reader.getRowGroupInfo(3).getNumberOfRows());

        

        }
        catch (IOException e)
        {
            e.printStackTrace();
        }

    }

}
