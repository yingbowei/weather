package com.youzidata.weather.util;

import java.text.SimpleDateFormat;

/**
 * @Author: YingBoWei
 * @Date: 2019-05-07 17:39
 * @Description:
 */
public class StringUtil {

    /**
     * 获取nc文件，文件名中的时间，yyyyMMddHH
     * 实时，单要素nc文件（示例：GRID_TJQX_PUB_DIS_AFTJ_000_DT_20180101080000_072-240_65.nc）
     * @param fileName
     * @return
     */
    public static String getDateStrFromNc(String fileName, int splitNum) {
        System.out.println(fileName);
        String [] strArr = fileName.split("_");
        String dateStr = strArr[7].substring(0, splitNum);
        return dateStr;
    }


    /**
     * 获取nc文件，文件名中的当前时效数
     * 实时，单要素nc文件（示例：GRID_TJQX_PUB_DIS_AFTJ_000_DT_20180101080000_072-240_65.nc）
     * @param fileName
     * @return
     */
    public static int getPreHourFromNc(String fileName) {
        String [] strArr = fileName.split("_");
        String preHour = strArr[8].split("-")[1];
        int preHourNum = Integer.parseInt(preHour);
        return preHourNum;
    }

    /**
     * 获取nc文件，文件名中的用户id
     * @param fileName
     * @return
     */
    public static String getUserIdFromNc(String fileName) {
        String [] strArr = fileName.split("_");
        String str = strArr[9];
        String user_id = str.substring(0, str.length() - 3);
        return user_id;
    }

    public static void main(String[] args) {
        String fileName = "GRID_TJQX_PUB_EDA_AFTJ_000_DT_20190506080000_072-240_701.nc";
        String user_id = getUserIdFromNc(fileName);
        System.out.println(user_id);
    }

}
