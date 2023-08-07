package helloworld;

import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.planner.plan.physical.domain.*;
import java.util.Arrays;
import java.util.ArrayList;

import java.util.List;

import java.util.HashMap;

public class Common {

    public static HashMap<String, List<String>> getTableToString(String pathpre,List<String> tables) {
        HashMap<String, List<String>> tableToInputSplits = new HashMap<String, List<String>>();
        for (String tableName : tables) {
        try {

                List<String> filePath = new ArrayList<String>();

                String storagepath = pathpre + tableName + "/" ;
                // System.out.println("storagepath: " + storagepath);
                Storage storage = StorageFactory.Instance().getStorage(storagepath);
                filePath = storage.listPaths(storagepath);
                System.out.println(filePath);
                tableToInputSplits.put(tableName, filePath);
                
        } catch (Exception e) {
                e.printStackTrace();
                return null;
        }
        }
        return tableToInputSplits;
    }

    
    public static HashMap<String, List<InputSplit>> getTableToInputSplits(List<String> tables) {
        HashMap<String, List<InputSplit>> tableToInputSplits = new HashMap<String, List<InputSplit>>();
        for (String tableName : tables) {
        try {
                List<InputSplit> paths = new ArrayList<InputSplit>();
                List<String> filePath = new ArrayList<String>();

                String storagepath = "s3://jingrong-lambda-test/tpch/" + tableName + "/" + "v-0-ordered" + "/";
                // System.out.println("storagepath: " + storagepath);
                Storage storage = StorageFactory.Instance().getStorage(storagepath);
                filePath = storage.listPaths(storagepath);

                for (String line : filePath) {
                        InputSplit temp = new InputSplit(Arrays.asList(new InputInfo(line, 0, -1)));
                        paths.add(temp);
                }

                tableToInputSplits.put(tableName, paths);
                
        } catch (Exception e) {
                e.printStackTrace();
                return null;
        }
        }
        return tableToInputSplits;
    }

}
