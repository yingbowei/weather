package com.youzidata.weather.controller;

import com.youzidata.weather.service.SearchSplitDataService;
import com.youzidata.weather.util.CheckParamUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author: YingBoWei
 * @Date: 2019-04-17 10:28
 * @Description:    大文件-18001*18001格点数据的nc文件查询
 */
@RestController
@CrossOrigin
public class SearchSplitDataController {

    @Autowired
    SearchSplitDataService searchSplitDataService;

    /**
     * 多线程，每个线程取一块rowkey的名为Himawari8的列
     * 抽希
     * 不写入文件、不合并
     * @param step_length   步长
     * @param gid_level 抽希级别（例如：100，表示100*100个格点取左上角一个格点）
     * @param startLat
     * @param endLat
     * @param startLon
     * @param endLon
     * @return
     */
    @GetMapping("/getData")
    public Object searchSplitData(@RequestParam(required = false) double step_length, @RequestParam(required = true) String table_name,
                                   @RequestParam(required = false) Integer gid_level, @RequestParam(required = true) Integer thread_num,
                                   @RequestParam(required = true) Double startLat, @RequestParam(required = true) Double endLat,
                                   @RequestParam(required = true) Double startLon, @RequestParam(required = true) Double endLon) {
        if(startLat > 90 || endLat < -90 || startLon < 60 || startLon > 240 || startLat > endLat || startLon > endLon) {
            CheckParamUtil.getErrorMap("1001");
        }
        Map<String, Object> resultMap = new HashMap<>();
        if(gid_level == null || gid_level == 0) {//不抽希
            resultMap = searchSplitDataService.statisticalQueryTest5(step_length, thread_num, startLat, endLat, startLon, endLon, table_name);
        }else {
            resultMap = searchSplitDataService.statisticalQueryTest9(step_length, gid_level, thread_num, startLat, endLat, startLon, endLon, table_name);
        }
        return resultMap;
    }

    /**
     * 多线程，每个线程取一块rowkey的名为Himawari8的列
     * 抽希
     * 不写入文件、不合并
     * @param step_length   步长
     * @param gid_level 抽希级别（例如：100，表示100*100个格点取左上角一个格点）
     * @param startLat
     * @param endLat
     * @param startLon
     * @param endLon
     * @return
     */
    @GetMapping("/getDataOld")
    public Object searchSplitDataString(@RequestParam(required = false) double step_length, @RequestParam(required = false) String table_name,
                                  @RequestParam(required = false) Integer gid_level, @RequestParam(required = true) Integer thread_num,
                                  @RequestParam(required = true) Double startLat, @RequestParam(required = true) Double endLat,
                                  @RequestParam(required = true) Double startLon, @RequestParam(required = true) Double endLon) {
        if(startLat > 90 || endLat < -90 || startLon < 60 || startLon > 240 || startLat > endLat || startLon > endLon) {
            CheckParamUtil.getErrorMap("1001");
        }
        Map<String, Object> resultMap = new HashMap<>();
        if(gid_level == null || gid_level == 0) {//不抽希
            resultMap = searchSplitDataService.statisticalQueryTest1(step_length, gid_level, thread_num, startLat, endLat, startLon, endLon, table_name);

        }else {//抽希
            resultMap = searchSplitDataService.statisticalQueryTest2(step_length, gid_level, thread_num, startLat, endLat, startLon, endLon, table_name);

        }
        return resultMap;
    }

    /**
     * 查询
     * 从存String的Hbase表中获取
     * 取最大值
     * @param step_length
     * @param table_name
     * @param gid_level
     * @param thread_num
     * @param startLat
     * @param endLat
     * @param startLon
     * @param endLon
     * @return
     */
    @GetMapping("/getMaxDataOld")
    public Object getMaxDataOld(@RequestParam(required = true) double step_length, @RequestParam(required = true) String table_name,
                                        @RequestParam(required = false) Integer gid_level, @RequestParam(required = true) Integer thread_num,
                                        @RequestParam(required = true) Double startLat, @RequestParam(required = true) Double endLat,
                                        @RequestParam(required = true) Double startLon, @RequestParam(required = true) Double endLon) {
        if(startLat > 90 || endLat < -90 || startLon < 60 || startLon > 240 || startLat > endLat || startLon > endLon) {
            CheckParamUtil.getErrorMap("1001");
        }
        Map<String, Object> resultMap = new HashMap<>();
        if(gid_level == null || gid_level == 0) {//不抽希
            resultMap = searchSplitDataService.statisticalQueryTest1(step_length, gid_level, thread_num, startLat, endLat, startLon, endLon, table_name);

        }else {//抽希
            resultMap = searchSplitDataService.getMaxDataOldDilution(step_length, gid_level, thread_num, startLat, endLat, startLon, endLon, table_name);

        }
        return resultMap;
    }

    /**
     * 查询
     * 从存float[]的Hbase表中获取
     * 取最大值
     * @param step_length
     * @param table_name
     * @param gid_level
     * @param thread_num
     * @param startLat
     * @param endLat
     * @param startLon
     * @param endLon
     * @return
     */
    @GetMapping("/getMaxData")
    public Object getMaxData(@RequestParam(required = false) double step_length, @RequestParam(required = false) String table_name,
                                @RequestParam(required = false) Integer gid_level, @RequestParam(required = true) Integer thread_num,
                                @RequestParam(required = true) Double startLat, @RequestParam(required = true) Double endLat,
                                @RequestParam(required = true) Double startLon, @RequestParam(required = true) Double endLon) {
        if(startLat > 90 || endLat < -90 || startLon < 60 || startLon > 240 || startLat > endLat || startLon > endLon) {
            CheckParamUtil.getErrorMap("1001");
        }
        Map<String, Object> resultMap = new HashMap<>();
        if(gid_level == null || gid_level == 0) {//不抽希
            resultMap = searchSplitDataService.statisticalQueryTest1(step_length, gid_level, thread_num, startLat, endLat, startLon, endLon, table_name);

        }else {//抽希
            resultMap = searchSplitDataService.getMaxDataDilution(step_length, gid_level, thread_num, startLat, endLat, startLon, endLon, table_name);

        }
        return resultMap;
    }

    /**
     * 测试由201809290350.nc文件切分导入的查询，先将目标矩阵块查出来进行抽希，然后按条件进行截取
     * @param startLat
     * @param endLat
     * @param startLon
     * @param endLon
     * @return
     */
//    @GetMapping("/searchSplitData1")
//    public Object searchSplitData1(@RequestParam(required = false) Integer gid_level,
//                                  @RequestParam(required = true) Double startLat, @RequestParam(required = true) Double endLat,
//                                  @RequestParam(required = true) Double startLon, @RequestParam(required = true) Double endLon) {
//        if(startLat > 90 || endLat < -90 || startLon < 60 || startLon > 240 || startLat > endLat || startLon > endLon) {
//            CheckParamUtil.getErrorMap("1001");
//        }
//        Map<String, Object> resultMap = searchSplitDataService.statisticalQueryTest1(gid_level, startLat, endLat, startLon, endLon);
//        return resultMap;
//    }

    /**
     * 多线程
     * 测试由201809290350.nc文件切分导入的查询，先将目标矩阵块查出来进行抽希，然后按条件进行截取
     * @param startLat
     * @param endLat
     * @param startLon
     * @param endLon
     * @return
     */
//    @GetMapping("/searchSplitData2")
//    public Object searchSplitData2(@RequestParam(required = false) Integer gid_level, @RequestParam(required = true) Integer thread_num,
//                                   @RequestParam(required = true) Double startLat, @RequestParam(required = true) Double endLat,
//                                   @RequestParam(required = true) Double startLon, @RequestParam(required = true) Double endLon) {
//        if(startLat > 90 || endLat < -90 || startLon < 60 || startLon > 240 || startLat > endLat || startLon > endLon) {
//            CheckParamUtil.getErrorMap("1001");
//        }
//        Map<String, Object> resultMap = searchSplitDataService.statisticalQueryTest2(gid_level, thread_num, startLat, endLat, startLon, endLon);
//        return resultMap;
//    }

    /**
     * 多线程
     * 进Hbase的时候，存入的是一维float数组
     * 测试由201809290350.nc文件切分导入的查询，先将目标矩阵块查出来进行抽希，然后按条件进行截取
     * @param step_length   步长
     * @param gid_level 抽希级别（例如：100，表示100*100个格点取左上角一个格点）
     * @param startLat
     * @param endLat
     * @param startLon
     * @param endLon
     * @return
     */
    @GetMapping("/searchSplitData3")
    public Object searchSplitData3(@RequestParam(required = false) double step_length,
                                    @RequestParam(required = false) Integer gid_level, @RequestParam(required = true) Integer thread_num,
                                   @RequestParam(required = true) Double startLat, @RequestParam(required = true) Double endLat,
                                   @RequestParam(required = true) Double startLon, @RequestParam(required = true) Double endLon) {
        if(startLat > 90 || endLat < -90 || startLon < 60 || startLon > 240 || startLat > endLat || startLon > endLon) {
            CheckParamUtil.getErrorMap("1001");
        }
        Map<String, Object> resultMap = searchSplitDataService.statisticalQueryTest3(step_length, gid_level, thread_num, startLat, endLat, startLon, endLon);
        return resultMap;
    }

    /**
     * 多线程嵌套多线程
     * 进Hbase的时候，存入的是一维float数组
     * 测试由201809290350.nc文件切分导入的查询，先将目标矩阵块查出来进行抽希，然后按条件进行截取
     * @param step_length   步长
     * @param gid_level 抽希级别（例如：100，表示100*100个格点取左上角一个格点）
     * @param startLat
     * @param endLat
     * @param startLon
     * @param endLon
     * @return
     */
    @GetMapping("/searchSplitData4")
    public Object searchSplitData4(@RequestParam(required = false) double step_length, @RequestParam(required = true) String table_name,
                                   @RequestParam(required = false) Integer gid_level, @RequestParam(required = true) Integer thread_num,
                                   @RequestParam(required = true) Double startLat, @RequestParam(required = true) Double endLat,
                                   @RequestParam(required = true) Double startLon, @RequestParam(required = true) Double endLon) {
        if(startLat > 90 || endLat < -90 || startLon < 60 || startLon > 240 || startLat > endLat || startLon > endLon) {
            CheckParamUtil.getErrorMap("1001");
        }
        Map<String, Object> resultMap = new HashMap<>();
        if(gid_level == null || gid_level == 0) {//不抽希
            resultMap = searchSplitDataService.statisticalQueryTest5(step_length, thread_num, startLat, endLat, startLon, endLon, table_name);
        }else {
            resultMap = searchSplitDataService.statisticalQueryTest9(step_length, gid_level, thread_num, startLat, endLat, startLon, endLon, table_name);
        }
        return resultMap;
    }
}
