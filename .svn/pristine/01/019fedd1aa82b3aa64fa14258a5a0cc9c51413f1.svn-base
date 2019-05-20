package com.youzidata.weather.controller;

import java.io.IOException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import com.youzidata.weather.service.LoadSingleElementFileService;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.youzidata.weather.task.LoadToHbaseTask;

/**
 * 单要素多时效的nc文件入库接口
 */
@RestController
@CrossOrigin
public class LoadDataController {

    @Autowired
    LoadSingleElementFileService loadSingleElementFileService;

    @Autowired
    LoadToHbaseTask loadToHbaseTask;
    @Autowired
    private HbaseTemplate hbaseTemplate;

    /**
     *
     * @param filePath
     * @return
     */
    @ResponseBody
    @GetMapping(value="/loadSingleElementFile")
    public  Object loadSingleElementFile(@RequestParam(required=true)String  filePath,
                                         @RequestParam(required=true)String startDate,
                                         @RequestParam(required=true)String endDate) throws ParseException {
        //新入库接口
//        Map<String, Object> resultMap = loadToHbaseTask.test(filePath, startDate, endDate);
        //老入库接口
        Map<String, Object> resultMap = loadSingleElementFileService.test(filePath, startDate, endDate);

        Map<String, Object> lastMap = new HashMap<>();
        lastMap.put("resultcode", 200);
        lastMap.put("resultMessage", "SUCCESS");
        lastMap.put("data", resultMap);
        return lastMap;
    }
}
