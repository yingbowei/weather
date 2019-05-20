package com.youzidata.weather.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @Author: YingBoWei
 * @Date: 2019-01-11 16:21
 * @Description:
 */
public class ListUtil {
//    //从Hbase取出来的矩阵，转换成以经度+纬度拼成的字符串作为key，指定字段的值作为value
//    public static Map<String,String> turnList(List<String> list_source){
//        Map<String,String> map_data = new HashMap<>();
//        List<List<String>> list_split=new ArrayList<>();//每行按,分割
//        for(String str:list_source){
//            List<String> list1= Arrays.asList(str.split(","));
//            list_split.add(list1);
//        }
//        String [][] str_split=new String [list_split.size()][list_split.get(0).size()];
//        for(int i=0;i<list_split.size();i++){
//            for(int j=0;j<list_split.get(i).size();j++){
//                str_split[i][j]=list_split.get(i).get(j);
//            }
//        }
//        String [] lon=new String[list_split.get(0).size()];//经度数组
//        String [] lat=new String[list_split.size()];//纬度数组
//        for(int i=0;i<str_split[0].length;i++){
//            lon[i]=str_split[0][i];
//        }
//        for(int i=0;i<str_split.length;i++){
//            lat[i]=str_split[i][0];
//        }
//
//        for(int i=1;i<str_split.length;i++){
//            for(int j=1;j<str_split[i].length;j++){
//                Map<String,Object> map_temp=new HashMap<>();
//                map_data.put(lon[j]+lat[i],str_split[i][j]);
//            }
//        }
//        return map_data;
//    }

    //转成二维数组
    public static List<List<String>> listToTwoDimensionalArray(List<String> list){
        List<List<String>> twoDimensionalArray=new ArrayList<>();
        for(String str:list){
            List<String> list1= Arrays.asList(str.split(","));
            twoDimensionalArray.add(list1);
        }
        return twoDimensionalArray;
    }
}
