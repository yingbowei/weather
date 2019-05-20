package com.youzidata.weather.schedule;

import com.youzidata.weather.service.LoadSingleElementFileService;
import com.youzidata.weather.util.DateUtil;
import com.youzidata.weather.util.FileUtil;
import com.youzidata.weather.util.TxtUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.File;
import java.text.ParseException;
import java.util.*;

/**
 * @Author: YingBoWei
 * @Date: 2019-05-18 13:23
 * @Description:
 */
@Component
public class LoadDataSchedule {
    @Value("${ScheduleParam.FilePath}")
    private String filePath;
    @Value("${ScheduleParam.TxtPath}")
    private String txtPath;
    @Autowired
    LoadSingleElementFileService loadSingleElementFileService;

    @Scheduled(cron = "0 0/5 * * * ?")
//    @Scheduled(cron = "0/5 * * * * ?")
    //或直接指定时间间隔，例如：5秒
    //@Scheduled(fixedRate=5000)
    public void loadData() {
        Date now = new Date();
        String startDate = DateUtil.formatDate(now, "yyyyMMdd");
//        String startDate = "20190506";
        String endDate = DateUtil.formatDate(DateUtil.addDate(now, 1), "yyyyMMdd");
//        String endDate = "20190507";
        List<String> dateStrArr = DateUtil.getDayRangeStrList(startDate, endDate, "yyyyMMdd", "yyyyMMdd");
        System.out.println(filePath);
        //和已经入库的文件列表进行比对，筛选出没有入库的新文件
        List<File> needInsertHbase = new ArrayList<>();
        List<File> filelist = FileUtil.getNewestFileAndNoUserId(filePath, startDate, endDate);//
        //获取已经入库的文件列表，并进行比对，找出新增的未入库的文件
        Map<String, String> alreadyInsertHbaseFile = new HashMap();
        for(String date:dateStrArr) {
            String wholePath = txtPath + File.separator + date + ".log";
            alreadyInsertHbaseFile.putAll(TxtUtil.readFile2(wholePath));
        }
//        System.out.println("-----已入库的文件列表");
//        for(Map.Entry<String, String> m:alreadyInsertHbaseFile.entrySet()) {
//            System.out.println(m.getKey());
//        }
        System.out.println("-----当前需要新增入库的文件列表");
        for(File file:filelist) {
            String path = file.getPath();
            if(!alreadyInsertHbaseFile.containsKey(path)) {
                System.out.println(path);
                needInsertHbase.add(file);
            }
        }
        Map<String, Object> resultMap = new HashMap<>();
        try {
            resultMap = loadSingleElementFileService.test(needInsertHbase);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        System.out.println(resultMap.toString());
    }
}
