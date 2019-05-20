package com.youzidata.weather.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * @Author: YingBoWei
 * @Date: 2019-04-26 10:14
 * @Description:
 */
public class DateUtil {
    public static void main(String[] args) {
        List<String> list = getDayRangeStrList("20190501", "20190510", "yyyyMMdd", "yyyyMMdd");
        for(String str:list) {
            System.out.println(str);
        }
        System.out.println("断点");
    }
    public static String formatDate(Date date, String return_format) {
        SimpleDateFormat sdf = new SimpleDateFormat(return_format);
        return sdf.format(date);
    }

    public static List<String> getDayRangeStrList(String start_date, String end_date, String format_src, String format_res) {
        List<String> list = new ArrayList<>();
        SimpleDateFormat sdf_src = new SimpleDateFormat(format_src);
        SimpleDateFormat sdf_res = new SimpleDateFormat(format_res);
        Date s_date = null;
        Date e_date = null;
        try {
            s_date = sdf_src.parse(start_date);
            e_date = sdf_src.parse(end_date);
            Calendar cal = Calendar.getInstance();
            cal.setTime(s_date);
            while(cal.getTime().compareTo(e_date) <= 0) {
                list.add(sdf_res.format(cal.getTime()));
                cal.add(Calendar.DATE, 1);
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }


        return list;
    }

    /**
     * 日期增加或减少n天，返回Date
     * @param date
     * @param num
     * @return
     */
    public static Date addDate(Date date, int num) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(Calendar.DATE, num);
        return cal.getTime();
    }
}
