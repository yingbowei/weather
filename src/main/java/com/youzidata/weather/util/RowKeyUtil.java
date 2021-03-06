package com.youzidata.weather.util;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: YingBoWei
 * @Date: 2019-04-18 9:39
 * @Description:
 */
public class RowKeyUtil {
    /**
     * 根据经度或者纬度获取rowkey数组，rowkey规则：date_layer_开始纬度_结束纬度_开始经度_结束经度,每个rowkey纬度间隔10度，经度间隔10度
     * @param start
     * @param end
     * @return  返回的是double_double的字符串数组
     */
    public static List<String> getRowKeyByLonOrLat(double start, double end) {
        List<String> list_return = new ArrayList<>();
        double temp_num1 = (int)start / 10 * 10.0d;
        double temp_num2;
        double head,tail;//根据start和end计算rowkey查询的最大区间范围
        if(temp_num1 <= 0 && start <= 0) {
            temp_num2 = temp_num1 - 10.0d;
            head = temp_num2;
            if(temp_num1 == start) {
                head = temp_num2 + 10.0d;
            }
        }else{
            head = temp_num1;
        }

        temp_num1 = (int)end / 10 * 10.0d;;
        if(temp_num1 <= 0 && end <= 0) {
            tail = temp_num1;
        }else{
            temp_num2 = temp_num1 + 10.0d;
            tail = temp_num2;
            if(temp_num1 == end) {
                tail = tail - 10.0;
            }
        }

        double start_num = tail - 10.0d;
        double end_num = tail;
        while(start_num >= head) {
            list_return.add((int)start_num + "_" + (int)end_num);
            start_num += -10.0d;
            end_num += -10.0d;
        }
        return list_return;
    }

    public static void main(String[] args) {
//        List<String> li = getRowKeyByLonOrLat(60, 240);
//        for(String str:li) {
//            System.out.println(str);
//        }
        int i = (int) ((10.0-3.23)/0.01 + 2);
        System.out.println(i);
    }
}
