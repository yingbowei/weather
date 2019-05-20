package com.youzidata.weather.controller;

import com.youzidata.weather.service.LoadDataOptimizeService;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.*;

/**
 * @Author: YingBoWei
 * @Date: 2019-05-08 17:33
 * @Description:    多要素单时效的nc文件入库接口
 */
@RestController
public class LoadDataOptimizeController {

    @Autowired
    LoadDataOptimizeService loadDataOptimizeService;

    @Autowired
    private HbaseTemplate hbaseTemplate;

    @ResponseBody
    @GetMapping(value="/testload")
    public  Object testload(@RequestParam(required=true)String  filePath) {
        Admin admin = null;
        Connection conn = null;
        try {
            String date = filePath.substring(filePath.length()-17, filePath.length()-7);
//            admin = DataSourceConfig.connection.getAdmin();
            conn = ConnectionFactory.createConnection(hbaseTemplate.getConfiguration());
            admin = conn.getAdmin();
            if (!admin.isTableAvailable(TableName.valueOf("weather_"+date))) {
                HTableDescriptor hbaseTable = new HTableDescriptor(TableName.valueOf("weather_"+date));
                hbaseTable.addFamily(new HColumnDescriptor("cf"));
                byte[][] splitKeys = getSplitKeys(date);
                admin.createTable(hbaseTable,splitKeys);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally{
            try {
                admin.close();
                conn.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        long  satrtTime = System.currentTimeMillis();
        Map<String, Object> lastMap = new HashMap<>();
        Map<String, Long> nanMap = new HashMap<>();
        Map<String, Long> writeMap = new HashMap<>();
        Map<String, Long> readMap = new HashMap<>();
        boolean load = loadDataOptimizeService.load(filePath,nanMap,writeMap,readMap);
        if(load) {
            Map<String, Object> dataMap = new HashMap<>();
            Set<Map.Entry<String, Long>> entrySet = nanMap.entrySet();
            entrySet.forEach(m ->{
                dataMap.put("analysisTime", m.getValue());
            });
            Set<Map.Entry<String, Long>> entrySet2 = writeMap.entrySet();
            entrySet2.forEach( o ->{
                dataMap.put("writeTime", o.getValue());
            });
            readMap.entrySet().forEach(o ->{
                dataMap.put("readTime", o.getValue());
            });
            long endtime = System.currentTimeMillis();
            System.out.println("==========一共消耗==========");
            dataMap.put("sumTime", endtime - satrtTime);
            lastMap.put("resultcode", 200);
            lastMap.put("resultMessage", "SUCCESS");
            lastMap.put("data", dataMap);
        }else {
            lastMap.put("resultcode", 10001);
            lastMap.put("resultMessage", "ERROR");
            lastMap.put("data", null);
        }

        return lastMap;
    }
    private byte[][] getSplitKeys(String date) {
        String[] keys = new String[] {  date+"120_", date+"147_", date+"171_", date+"195_", date+"219_",date+"27_",date+"51",date+"75"};
        byte[][] splitKeys = new byte[keys.length][];
        TreeSet<byte[]> rows = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);//升序排序
        for (int i = 0; i < keys.length; i++) {
            rows.add(Bytes.toBytes(keys[i]));
        }
        Iterator<byte[]> rowKeyIter = rows.iterator();
        int i=0;
        while (rowKeyIter.hasNext()) {
            byte[] tempRow = rowKeyIter.next();
            rowKeyIter.remove();
            splitKeys[i] = tempRow;
            i++;
        }
        return splitKeys;
    }

    @ResponseBody
    @GetMapping(value="/testloaddir")
    public  Object testloaddir(@RequestParam(required=true)String  filePath) {
         Map<String, Object> resultMap = loadDataOptimizeService.test(filePath);
        Map<String, Object> lastMap = new HashMap<>();
        lastMap.put("resultcode", 200);
        lastMap.put("resultMessage", "SUCCESS");
         lastMap.put("data", resultMap);
        return lastMap;
    }
}
