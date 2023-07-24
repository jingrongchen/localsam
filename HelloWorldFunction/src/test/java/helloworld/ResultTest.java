package helloworld;

import io.pixelsdb.pixels.common.metadata.domain.Schema;
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
        String currentPath = "s3://jingrong-test/tpch/customer/v-0-order/20230425092143_0.pxl";
        try {

            StorageFactory storagefactory = StorageFactory.Instance();

            Storage storage =storagefactory.getStorage(currentPath);
            
            PixelsReader reader=WorkerCommon.getReader(currentPath,storage);
           
            TypeDescription schema = reader.getFileSchema();
            List<String> fieldNames = schema.getFieldNames();
            String[] cols = new String[fieldNames.size()];
            for (int i = 0; i < fieldNames.size(); i++) {
                cols[i] = fieldNames.get(i);
            }

            PixelsReaderOption option = new PixelsReaderOption();
            option.skipCorruptRecords(true);
            option.tolerantSchemaEvolution(true);
            option.includeCols(cols);
            PixelsRecordReader recordReader = reader.read(option);
            System.out.println(recordReader.getCompletedRows());
            // System.out.println(reader.getRowGroupInfo(0).getNumberOfRows());
            int batchSize = 10;
            VectorizedRowBatch rowBatch;
            int len = 0;
            int num = 0;
            int row = 0;

            for(int i=0;i<10;i++){
                rowBatch = recordReader.readBatch(batchSize);
                System.out.println(rowBatch);
                row++;
                String result = rowBatch.toString();
                len += result.length();
                System.out.println("loop:" + row + "," + rowBatch.size);
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

}
