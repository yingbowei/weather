package com.youzidata.weather.service;

import com.youzidata.weather.dao.HbaseDao;
import com.youzidata.weather.util.*;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.data.hadoop.hbase.TableCallback;
import org.springframework.stereotype.Service;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @Author: YingBoWei
 * @Date: 2019-04-17 11:04
 * @Description:
 */
@Service
public class SearchSplitDataService {
    @Autowired
    private HbaseTemplate hbaseTemplate;

    @Autowired
    private HbaseDao hbaseDao;

    @Value("${CSVPath}")
    private String CSVPath;

    private static byte[] family = "cf".getBytes();

    /**
     * 统计查询时间、时间等
     * @param startLat
     * @param endLat
     * @param startLon
     * @param endLon
     * @return
     */
    public Map<String, Object> statisticalQuery(Integer Integer, Double startLat, Double endLat, Double startLon, Double endLon) {
        Map<String, Object> map_return = new HashMap<>();
        String rowkey_prefix = "2018092903_0_";
        //计算开始和结束纬度之间，需要在hbase中查询的所有rowkey的纬度拼接部分
        List<String> rowkey_lat = RowKeyUtil.getRowKeyByLonOrLat(startLat,endLat);
        //计算开始和结束经度之间，需要在hbase中查询的所有rowkey的经度拼接部分
        List<String> rowkey_lon = RowKeyUtil.getRowKeyByLonOrLat(startLon,endLon);
        //        CSVUtil util = new CSVUtil();//生成csv文件的工具类
//        DoubleUtil doubleUtil = new DoubleUtil();//设置数字精度的工具类
        long start = new Date().getTime();//统计时长-开始时间
//        long time = System.currentTimeMillis();//获取当前时间，用来组成生成的文件名
        Map<String, List<List<String>>> row_map = new LinkedHashMap<>(); //每一级的纬度对应的全部rowkey，拼起来的数据块
        Connection conn = null;
        Table table = null;
        try {
            conn = ConnectionFactory.createConnection(hbaseTemplate.getConfiguration());
            TableName tableName = TableName.valueOf("weather_2018092903");
            table = conn.getTable(tableName);
            for(String str:rowkey_lat) {
//                Scan scan = new Scan();
//                RowFilter filter1= new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL, new BinaryComparator(Bytes.toBytes(rowkey_prefix+str+"_"+rowkey_lon.get(0))));//大于等于
//                RowFilter filter2= new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes(rowkey_prefix+str+"_"+rowkey_lon.get(rowkey_lon.size()-1))));//小于等于
//                System.out.println(rowkey_prefix+str+"_"+rowkey_lon.get(0));
//                System.out.println(rowkey_prefix+str+"_"+rowkey_lon.get(rowkey_lon.size()-1));
//                scan.setFilter(filter1);
//                scan.setFilter(filter2);
//                scan.setStartRow(Bytes.toBytes(rowkey_prefix+str+"_"+rowkey_lon.get(0)));
//                scan.setStopRow(Bytes.toBytes(rowkey_prefix+str+"_"+rowkey_lon.get(rowkey_lon.size()-1)));
//                ResultScanner scanner = table.getScanner(scan);
//                for (Result res : scanner) {
//                    System.out.println(res);
//                }
//                scanner.close();
                List<Get> getList = new ArrayList<>();
                List<List<String>> row = new ArrayList<>();//二维数组
                for(String str_lon:rowkey_lon) {
                    Get get = new Get(Bytes.toBytes(rowkey_prefix+str+"_"+str_lon));
                    get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("Himawari8"));
                    getList.add(get);
                }
                Result[] resarr = table.get(getList);
                for (int i = 0; i < resarr.length; i++) {
                    Result result = resarr[i];
                    int row_index = 1;//按行读取hbase中的矩阵，记录当前读取行数
                    BufferedReader br = new BufferedReader(
                            new InputStreamReader(new ByteArrayInputStream(result.value())));
                    String line;
                    while ((line = br.readLine()) != null) {
                        if(row_index > 1) {
                            List<String> row_strs = Arrays.asList(line.split(","));
                            row_strs = row_strs.subList(1,row_strs.size());
                            if(i > 0) {//同一纬度，不通经度的矩阵，为了防止有重复数据，从第二个矩阵块开始舍弃一列数字
                                row_strs = row_strs.subList(1,row_strs.size());
                            }
                            if(i == 0) {
                                row.add(row_strs);
                            }else{
                                row.get(row_index - 2).addAll(row_strs);
                            }
                        }
                        row_index++;
                    }
                }
                row_map.put(str,row);
            }


        } catch (IOException e) {
            e.printStackTrace();
        }


        return map_return;
    }

    public Map<String, Object> statisticalQueryTest(Integer gid_level, Double startLat,
            Double endLat, Double startLon, Double endLon) {
        Map<String, Object> map_return = new HashMap<>();
        String rowkey_prefix = "2018092903_0_";
        //计算开始和结束纬度之间，需要在hbase中查询的所有rowkey的纬度拼接部分
        List<String> rowkey_lat = RowKeyUtil.getRowKeyByLonOrLat(startLat,endLat);
        //计算开始和结束经度之间，需要在hbase中查询的所有rowkey的经度拼接部分
        List<String> rowkey_lon = RowKeyUtil.getRowKeyByLonOrLat(startLon,endLon);

        int lat_first_index;//第一个纬度范围的矩阵，纬度开始的索引
        int lat_final_index;//最后一个纬度范围的矩阵，纬度结束的索引
        int lon_first_index;//第一个经度范围的矩阵，纬度开始的索引
        int lon_final_index;//最后一个经度范围的矩阵，纬度结束的索引
        //计算
        String [] rowkey_lat_first = rowkey_lat.get(0).split("_");
        lat_first_index = (int)((Double.parseDouble(rowkey_lat_first[1]) - endLat)/0.01 + 2 - 1);
        String [] rowkey_lat_final = rowkey_lat.get(rowkey_lat.size() - 1).split("_");
        lat_final_index = (int)((Double.parseDouble(rowkey_lat_final[0]) - startLat)/0.01 + 2 - 1);
        String [] rowkey_lon_first = rowkey_lon.get(0).split("_");
        lon_first_index = (int)((startLon - Double.parseDouble(rowkey_lon_first[0]))/0.01 + 2 - 1);;
        String [] rowkey_lon_final = rowkey_lon.get(rowkey_lon.size() - 1).split("_");
        lon_final_index = (int)((endLon - Double.parseDouble(rowkey_lon_final[0]))/0.01 + 2 - 1);

        List<List<String>> row_all = new ArrayList<>(); //每个纬度范围的数据块拼成最终的一个list，进行抽希
        Connection conn = null;
        Table table = null;
        Long selectTime = 0l;
        try {
            conn = ConnectionFactory.createConnection(hbaseTemplate.getConfiguration());
            TableName tableName = TableName.valueOf("weather_2018092903");
            table = conn.getTable(tableName);
            selectTime = System.currentTimeMillis();
            for(int i = 0; i < rowkey_lat.size(); i++) {
                String[] rowkey_lat_strs = rowkey_lat.get(i).split("_");//获取一个rowkey中纬度的端点
                String rowkey_lat_str = rowkey_lat.get(i);//获取一个rowkey中纬度的范围
                List<Get> getList = new ArrayList<>();
                List<List<String>> row = new ArrayList<>();//二维数组，存储一个纬度范围内的所有矩阵块
                for(String rowkey_lon_str:rowkey_lon) {
                    Get get = new Get(Bytes.toBytes(rowkey_prefix+rowkey_lat_str+"_"+rowkey_lon_str));
                    get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("Himawari8"));
                    getList.add(get);
                }
                Result[] resarr = table.get(getList);//一个纬度范围内的所有矩阵块数据
                int  startLatIndex,endLatIndex;//当前这个纬度范围内，在读取矩阵并转化为二维数组后的行起始索引
                if(rowkey_lat.size() == 1) {//只有一个纬度范围
                    startLatIndex = lat_first_index;
                    endLatIndex = lat_final_index;

                }else{//纬度范围大于一个
                    if(i == 0) {//查询的是第一个纬度范围
                        startLatIndex = lat_first_index;
                        endLatIndex = 1001;
                    }else if(i == rowkey_lat.size() - 1){//查询的是最后一个纬度范围
                        startLatIndex = 2;
                        endLatIndex = lat_final_index;
                    }else {//查询除了第一个和最后一个纬度范围
                        startLatIndex = 2;
                        endLatIndex = 1001;
                    }
                }
                //同一个纬度范围内，遍历所有经度范围的矩阵块
                for (int j = 0; j < resarr.length; j++) {
                    String[] rowkey_lon_strs = rowkey_lon.get(j).split("_");//获取一个rowkey中经度的范围
                    String rowkey_lon_str = rowkey_lon.get(j);//获取一个rowkey中经度的两个端点值
                    int startLonIndex, endLonIndex;//当前这个经度范围内，在读取矩阵并转化为二维数组后的行起始索引
                    if(resarr.length == 1) {//同一纬度范围内，只有一个经度范围
                        startLonIndex = lon_first_index;
                        endLonIndex = lon_final_index;
                    }else {//同一纬度范围内，经度范围大于一个
                        if(j == 1){//当前查询的是第一个经度范围
                            startLonIndex = lon_first_index;
                            endLonIndex = 1001;
                        }else if(j == resarr.length - 1) {//当前查询的是最后一个经度范围
                            startLonIndex = 2;
                            endLonIndex = lon_final_index;
                        }else {//除了第一个经度范围和最后一个经度范围
                            startLonIndex = 2;
                            endLonIndex = 1001;
                        }
                    }
                    Result result = resarr[j];
                    BufferedReader br = new BufferedReader(
                            new InputStreamReader(new ByteArrayInputStream(result.value())));
                    String line;
                    int row_index = 0;//按行读取hbase中的矩阵，记录当前读取行数,第一行索引为0
                    while ((line = br.readLine()) != null) {
                        List<String> row_strs = Arrays.asList(line.split(","));
                        if(row_index >= startLatIndex && row_index <= endLatIndex) {
                            row_strs = row_strs.subList(startLonIndex,endLonIndex + 1);
                            if(j == 0) {//同一纬度范围，第一个经度范围
                                row.add(row_strs);
                            }else{//同一纬度范围，除了第一个经度范围
                                row.get(row_index - startLatIndex).addAll(row_strs);
                            }

                        }
                        row_index++;
                    }

                }

                row_all.addAll(row);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("查询时间："+ (System.currentTimeMillis() - selectTime));

        //开始抽希

        return map_return;
    }

    /**
     * 优化之前
     * 存储类型：String类型的byte[]
     * 不抽希，不写入文件，合并
     * 先将目标矩阵块查出来进行抽希，然后按条件进行截取，多线程
     * @param gid_level
     * @param startLat
     * @param endLat
     * @param startLon
     * @param endLon
     * @return
     */
    public Map<String, Object> statisticalQueryTest1(double step_length, int gid_level, Integer thread_num, Double startLat,
                                                     Double endLat, Double startLon, Double endLon, String table_name) {
        Map<String, Object> map_return = new HashMap<>();
        //生成的csv文件列表的父目录
        String father_dir = CSVPath + DateUtil.formatDate(new Date(), "yyyyMMddHHmmss") + "/";
        String rowkey_prefix = "2018092903_0_";
        int split_interval = (int)(10.0/step_length) + 1;//计算每行，每列有多少格点数
        int split_interval_clean = (int)(10.0/(step_length * gid_level)) + 1;////计算抽希后，每行，每列有多少格点数
        //计算开始和结束纬度之间，需要在hbase中查询的所有rowkey的纬度拼接部分，升序
        List<String> rowkey_lat = RowKeyUtil.getRowKeyByLonOrLat(startLat,endLat);
        //计算开始和结束经度之间，需要在hbase中查询的所有rowkey的经度拼接部分，升序
        List<String> rowkey_lon = RowKeyUtil.getRowKeyByLonOrLat(startLon,endLon);
        Collections.reverse(rowkey_lon);//纬度范围变为降序
        //第一个纬度范围，开始的纬度坐标
        double start_lat = Double.parseDouble(rowkey_lat.get(0).split("_")[1]);
        //最后一个纬度范围，开始的纬度坐标
        double end_lat = Double.parseDouble(rowkey_lat.get(rowkey_lat.size() - 1).split("_")[1]);
        //第一个经度范围，开始的经度坐标，开始索引
        double start_lon = Double.parseDouble(rowkey_lon.get(0).split("_")[0]);
        //最后一个经度范围，开始的经度坐标，开始索引
        double end_lon = Double.parseDouble(rowkey_lon.get(rowkey_lon.size() - 1).split("_")[0]);

        Map<String, Long> map_all_select_time = new HashMap<>();//所有线程的查询时间
        long final_time_start = System.currentTimeMillis();//计算总时间-开始时间
        long final_time_total = 0l;//计算总时间-总时间
        ExecutorService service = Executors.newFixedThreadPool(thread_num);//线程池

        for(int i = 0; i < rowkey_lat.size(); i++) {
            final int i_run = i;
            //计算，在该纬度范围内的所有矩阵，纬度的开始索引和结束索引
            int start_lat_index = 0;
            int end_lat_index = split_interval_clean - 1;
            if(rowkey_lat.size() == 1) {
                end_lat_index = (int)CalculateUtil.divide(CalculateUtil.subtract(start_lat, startLat), step_length, 2);
                start_lat_index = (int)CalculateUtil.divide(CalculateUtil.subtract(end_lat, endLat), step_length, 2);
            }else {
                if(i_run == 0) {
                    start_lat_index = (int)CalculateUtil.divide(CalculateUtil.subtract(start_lat, endLat), step_length, 2);
                }else if(i_run == rowkey_lat.size() - 1) {
                    end_lat_index = (int)CalculateUtil.divide(CalculateUtil.subtract(end_lat, startLat), step_length, 2);
                }
            }
            final int final_end_lat_index = end_lat_index;
            final int final_start_lat_index = start_lat_index;
            //同一个纬度范围内，遍历所有经度范围的矩阵块
            for (int j = 0; j < rowkey_lon.size(); j++) {
                final int j_run = j;
                int start_lon_index = 0;
                int end_lon_index = split_interval_clean - 1;
                if(rowkey_lon.size() == 1) {
                    start_lon_index = (int)CalculateUtil.divide(CalculateUtil.subtract(startLon, start_lon), step_length, 2);
                    end_lon_index = (int)CalculateUtil.divide(CalculateUtil.subtract(endLon, end_lon), step_length, 2);
                }else {
                    if(j_run == 0) {
                        start_lon_index = (int)CalculateUtil.divide(CalculateUtil.subtract(start_lon, startLon), step_length, 2);
                    }else if(j_run == rowkey_lon.size() - 1) {
                        end_lon_index = (int)CalculateUtil.divide(CalculateUtil.subtract(end_lon, endLon), step_length, 2);
                    }
                }
                final int final_start_lon_index = start_lon_index;
                final int final_end_lon_index = end_lon_index;
                service.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            Connection conn = ConnectionFactory.createConnection(hbaseTemplate.getConfiguration());
                            TableName tableName = TableName.valueOf(table_name);
                            Table table = conn.getTable(tableName);
                            Get get = new Get(Bytes.toBytes(rowkey_prefix+rowkey_lat.get(i_run)+"_"+rowkey_lon.get(j_run) + "_" + step_length));
                            long select_time_start = System.currentTimeMillis();//查询开始时间
                            Result result = table.get(get);
                            long select_time_total = System.currentTimeMillis() - select_time_start;//单个矩阵
                            map_all_select_time.put(i_run+"_"+"j_run", select_time_total);
                            List<List<String>> row_arr2 = new ArrayList();//抽希前

                            BufferedReader br = new BufferedReader(
                                    new InputStreamReader(new ByteArrayInputStream(result.value())));
                            String line;
                            int line_index = 0;//按行读取hbase中的矩阵，记录当前读取行数,第一行索引为0
                            while ((line = br.readLine()) != null) {
                                if(line_index > 0) {
                                    List<String> row_strs = Arrays.asList(line.split(","));
                                    row_strs = row_strs.subList(1, row_strs.size());
                                    row_arr2.add(row_strs);
                                }
                                line_index++;
                            }
                            String path = father_dir+i_run+"_"+j_run+".csv";
//                                            CSVUtil.FloatArray2Csv(float_arr2, path);
                            table.close();
                            conn.close();

                        }catch(Exception ex) {
                            ex.printStackTrace();
                        }
                    }
                });
            }
        }
        service.shutdown();
        while(true){
            if(service.isTerminated()){
                System.out.println("一个子线程结束");
                break;
            }
        }
        final_time_total = System.currentTimeMillis() - final_time_start;
        map_return.put("total_time", final_time_total + "ms");
        for(Map.Entry<String, Long> m:map_all_select_time.entrySet()) {
            System.out.println("查询时间："+m.getValue());
        }
        return map_return;
    }


    /**
     * 优化之前
     * 存储类型：String类型的byte[]
     * 抽希,取左上角的格点，不写入文件，合并
     * 先将目标矩阵块查出来进行抽希，然后按条件进行截取，多线程
     * @param gid_level
     * @param startLat
     * @param endLat
     * @param startLon
     * @param endLon
     * @return
     */
    public Map<String, Object> statisticalQueryTest2(double step_length, int gid_level, Integer thread_num, Double startLat,
                                                     Double endLat, Double startLon, Double endLon, String table_name) {
        Map<String, Object> map_return = new HashMap<>();
        //生成的csv文件列表的父目录
        String father_dir = CSVPath + DateUtil.formatDate(new Date(), "yyyyMMddHHmmss") + "/";
        String rowkey_prefix = "2018092903_0_";
        int split_interval = (int)(10.0/step_length) + 1;//计算每行，每列有多少格点数
        int split_interval_clean = (int)(10.0/(step_length * gid_level)) + 1;////计算抽希后，每行，每列有多少格点数
        //计算开始和结束纬度之间，需要在hbase中查询的所有rowkey的纬度拼接部分，升序
        List<String> rowkey_lat = RowKeyUtil.getRowKeyByLonOrLat(startLat,endLat);
        //计算开始和结束经度之间，需要在hbase中查询的所有rowkey的经度拼接部分，升序
        List<String> rowkey_lon = RowKeyUtil.getRowKeyByLonOrLat(startLon,endLon);
        Collections.reverse(rowkey_lon);//纬度范围变为降序
        //第一个纬度范围，开始的纬度坐标
        double start_lat = Double.parseDouble(rowkey_lat.get(0).split("_")[1]);
        //最后一个纬度范围，开始的纬度坐标
        double end_lat = Double.parseDouble(rowkey_lat.get(rowkey_lat.size() - 1).split("_")[1]);
        //第一个经度范围，开始的经度坐标，开始索引
        double start_lon = Double.parseDouble(rowkey_lon.get(0).split("_")[0]);
        //最后一个经度范围，开始的经度坐标，开始索引
        double end_lon = Double.parseDouble(rowkey_lon.get(rowkey_lon.size() - 1).split("_")[0]);

        Map<String, Long> map_all_select_time = new HashMap<>();//所有线程的查询时间
        long final_time_start = System.currentTimeMillis();//计算总时间-开始时间
        long final_time_total = 0l;//计算总时间-总时间
        ExecutorService service = Executors.newFixedThreadPool(thread_num);//线程池

        for(int i = 0; i < rowkey_lat.size(); i++) {
            final int i_run = i;
            //计算，在该纬度范围内的所有矩阵，纬度的开始索引和结束索引
            int start_lat_index = 0;
            int end_lat_index = split_interval_clean - 1;
            if(rowkey_lat.size() == 1) {
                end_lat_index = (int)CalculateUtil.divide(CalculateUtil.subtract(start_lat, startLat), step_length * gid_level, 2);
                start_lat_index = (int)CalculateUtil.divide(CalculateUtil.subtract(end_lat, endLat), step_length * gid_level, 2);
            }else {
                if(i_run == 0) {
                    start_lat_index = (int)CalculateUtil.divide(CalculateUtil.subtract(start_lat, endLat), step_length * gid_level, 2);
                }else if(i_run == rowkey_lat.size() - 1) {
                    end_lat_index = (int)CalculateUtil.divide(CalculateUtil.subtract(end_lat, startLat), step_length * gid_level, 2);
                }
            }
            final int final_end_lat_index = end_lat_index;
            final int final_start_lat_index = start_lat_index;
            //同一个纬度范围内，遍历所有经度范围的矩阵块
            for (int j = 0; j < rowkey_lon.size(); j++) {
                final int j_run = j;
                int start_lon_index = 0;
                int end_lon_index = split_interval_clean - 1;
                if(rowkey_lon.size() == 1) {
                    start_lon_index = (int)CalculateUtil.divide(CalculateUtil.subtract(startLon, start_lon), step_length * gid_level, 2);
                    end_lon_index = (int)CalculateUtil.divide(CalculateUtil.subtract(endLon, end_lon), step_length * gid_level, 2);
                }else {
                    if(j_run == 0) {
                        start_lon_index = (int)CalculateUtil.divide(CalculateUtil.subtract(start_lon, startLon), step_length * gid_level, 2);
                    }else if(j_run == rowkey_lon.size() - 1) {
                        end_lon_index = (int)CalculateUtil.divide(CalculateUtil.subtract(end_lon, endLon), step_length * gid_level, 2);
                    }
                }
                final int final_start_lon_index = start_lon_index;
                final int final_end_lon_index = end_lon_index;
                service.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            Connection conn = ConnectionFactory.createConnection(hbaseTemplate.getConfiguration());
                            TableName tableName = TableName.valueOf(table_name);
                            Table table = conn.getTable(tableName);
                            Get get = new Get(Bytes.toBytes(rowkey_prefix+rowkey_lat.get(i_run)+"_"+rowkey_lon.get(j_run) + "_" + step_length));
                            long select_time_start = System.currentTimeMillis();//查询开始时间
                            Result result = table.get(get);
                            long select_time_total = System.currentTimeMillis() - select_time_start;//单个矩阵
                            map_all_select_time.put(i_run+"_"+"j_run", select_time_total);
                            List<List<String>> row_arr2 = new ArrayList();//抽希前
                            List<List<String>> row_arr2_clean = new ArrayList();//抽希后

                            BufferedReader br = new BufferedReader(
                                    new InputStreamReader(new ByteArrayInputStream(result.value())));
                            String line;
                            int line_index = 0;//按行读取hbase中的矩阵，记录当前读取行数,第一行索引为0
                            while ((line = br.readLine()) != null) {
                                if(line_index > 0) {
                                    List<String> row_strs = Arrays.asList(line.split(","));
                                    row_strs = row_strs.subList(1, row_strs.size());
                                    row_arr2.add(row_strs);
                                }
                                line_index++;
                            }
                            for(int i = 0; i < row_arr2.size(); i++) {
                                if(i % gid_level == 0) {
                                    List<String> li = row_arr2.get(i);
                                    List<String> li_temp = new ArrayList<>();
                                    for(int j = 0; j < li.size(); j++) {
                                        if(j % gid_level == 0) {
                                            li_temp.add(li.get(j));
                                        }
                                    }
                                    row_arr2_clean.add(li_temp);
                                }
                            }
                            String path = father_dir+i_run+"_"+j_run+".csv";
//                                            CSVUtil.FloatArray2Csv(float_arr2, path);
                            table.close();
                            conn.close();

                        }catch(Exception ex) {
                            ex.printStackTrace();
                        }
                    }
                });
            }
        }
        service.shutdown();
        while(true){
            if(service.isTerminated()){
                System.out.println("一个子线程结束");
                break;
            }
        }
        final_time_total = System.currentTimeMillis() - final_time_start;
        map_return.put("total_time", final_time_total + "ms");
        for(Map.Entry<String, Long> m:map_all_select_time.entrySet()) {
            System.out.println("查询时间："+m.getValue());
        }
        return map_return;
    }

    /**
     * 从Hbase查询，直接转为一维float数组，然后转为二维float数组
     * 先将目标矩阵块查出来进行抽希，然后按条件进行截取，多线程
     * @param gid_level
     * @param startLat
     * @param endLat
     * @param startLon
     * @param endLon
     * @return
     */
    public Map<String, Object> statisticalQueryTest3(double step_length, Integer gid_level, Integer thread_num, Double startLat,
                                                     Double endLat, Double startLon, Double endLon) {
        Map<String, Object> map_return = new HashMap<>();
        String rowkey_prefix = "2018092903_0_";
        int split_interval = (int)(10.0/step_length) + 1;//计算每行有多少格点数
        //计算开始和结束纬度之间，需要在hbase中查询的所有rowkey的纬度拼接部分
        List<String> rowkey_lat = RowKeyUtil.getRowKeyByLonOrLat(startLat,endLat);
        //计算开始和结束经度之间，需要在hbase中查询的所有rowkey的经度拼接部分
        List<String> rowkey_lon = RowKeyUtil.getRowKeyByLonOrLat(startLon,endLon);
        Collections.reverse(rowkey_lon);
        //从Hbase查询出的所有矩阵，抽希完之后的经纬度开始点
        double startLatTogether = Double.parseDouble(rowkey_lat.get(0).split("_")[1]);
        double startLonTogether = Double.parseDouble(rowkey_lon.get(0).split("_")[0]);;

        List<List<Float>> row_all = new ArrayList<>(); //每个纬度范围的数据块拼成最终的一个list，进行抽希
        TreeMap<Integer, List<List<Float>>> row_all_map=new TreeMap<Integer, List<List<Float>>>(new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                return o1.compareTo(o2);
            }
        });
        Connection conn = null;
        Table table = null;
        Long selectTime = 0l;
        ExecutorService executor = Executors.newFixedThreadPool(thread_num);
        Map<Integer, Long> selectTimeMap = new HashMap<>();//记录所有线程的查询时间
        Map<Integer, Long> chouxiTimeMap = new HashMap<>();//记录所有线程的抽希时间

        try {
            conn = ConnectionFactory.createConnection(hbaseTemplate.getConfiguration());
            TableName tableName = TableName.valueOf("weather_2018092903");
            table = conn.getTable(tableName);
            selectTime = System.currentTimeMillis();
            for(int i = 0; i < rowkey_lat.size(); i++) {
                final int i_run = i;
                final Table table_run = table;
                executor.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            String rowkey_lat_str = rowkey_lat.get(i_run);//获取一个rowkey中纬度的范围
                            List<Get> getList = new ArrayList<>();
                            List<List<Float>> row_clean = new ArrayList<>();//二维数组，存储一个纬度范围内的所有矩阵块，抽希后
                            for(String rowkey_lon_str:rowkey_lon) {
                                Get get = new Get(Bytes.toBytes(rowkey_prefix+rowkey_lat_str+"_"+rowkey_lon_str + "_" + step_length));
                                get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("Himawari8"));
                                getList.add(get);
                            }
                            long selectTime = 0l;//当前线程中，查询Hbase时间
                            long chouxiTime = 0l;//当前线程中，抽希的时间
                            //同一个纬度范围内，遍历所有经度范围的矩阵块
                            for (int j = 0; j < getList.size(); j++) {
                                List<List<Float>> temp_row = new ArrayList();//Hbase中存储的每一块原始矩阵数据
                                long selectTimeStart = System.currentTimeMillis();
                                Result result = table_run.get(getList.get(j));//从Hbase查询
                                selectTime += System.currentTimeMillis() - selectTimeStart;

                                long chouxiTimeStart = System.currentTimeMillis();
                                float[] arr_float = ConversionUtil.bytesToFloat(result.value());//将Hbase中的字节类型转为float数组
                                int split_num = arr_float.length / split_interval;
                                for(int s = 0; s < split_num; s++) {
                                    List<Float> l_temp = new ArrayList<>();
                                    for(int ss=s*split_interval; ss < (s+1)*split_interval; ss++) {
                                        l_temp.add(arr_float[ss]);
                                    }
                                    temp_row.add(l_temp);
                                }
                                //计算抽希需要循环多少次
                                int cycle_number = temp_row.size()/gid_level;
                                //开始抽希
                                for(int n = 0; n < cycle_number; n++) {//遍历每一行
                                    List arr_temp1 = new ArrayList<>();
                                    for(int nn = 0; nn < cycle_number; nn++) {
                                        arr_temp1.add(temp_row.get(n * gid_level).get(nn * gid_level));
                                    }
                                    if(j == getList.size() - 1) {//同一纬度范围，当前是最后一个经度范围的时候
                                        arr_temp1.add(temp_row.get(n*gid_level).get(temp_row.size() - 1));
                                    }

                                    if(j == 0) {//同一纬度范围，当前是第一个经度范围的时候
                                        row_clean.add(arr_temp1);
                                    }else{//同一纬度范围，除了第一个经度范围的时候
                                        row_clean.get(n).addAll(arr_temp1);
                                    }
                                }
                                if(i_run == rowkey_lat.size() - 1) {//如果是最后一个纬度范围，所有经度范围抽希结果需要加上最后一行
                                    List ll = new ArrayList();//存储最后一行抽希结果
                                    for(int n = 0; n < cycle_number; n++) {
                                        ll.add(temp_row.get(temp_row.size() - 1).get(n * gid_level));
                                    }
                                    ll.add(temp_row.get(temp_row.size() - 1).get(temp_row.size() - 1));
                                    if(j == 0) {
                                        row_clean.add(ll);
                                    }else{
                                        row_clean.get(row_clean.size() - 1).addAll(ll);
                                    }
                                }
                                chouxiTime += System.currentTimeMillis() - chouxiTimeStart;
                            }
                            selectTimeMap.put(i_run, selectTime);
                            chouxiTimeMap.put(i_run, chouxiTime);
                            row_all_map.put(i_run, row_clean);
                        }catch(IOException ex) {
                            ex.printStackTrace();
                        }
                    }
                });

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        executor.shutdown();
        while(true){
            if(executor.isTerminated()){
                System.out.println("-----所有的子线程都结束了！");
                break;
            }
        }

        //多线程抽希完的矩阵合并
        long hebingTimeStart = System.currentTimeMillis();
        for(Map.Entry<Integer, List<List<Float>>> m:row_all_map.entrySet()) {
            row_all.addAll(m.getValue());
        }
        long hebingTime = System.currentTimeMillis() - hebingTimeStart;
        System.out.println("抽希完的矩阵合并时间："+hebingTime+"ms");
        long selectAndExtraction = System.currentTimeMillis() - selectTime;
        System.out.println("查询+抽希时间："+ selectAndExtraction);
        //从抽希完的二维数组中，截取查询结果
        long chouxiTimeStart = System.currentTimeMillis();
//        int endLatIndex = (int)((startLatTogether - startLat)/(0.01 * gid_level));
//        int startLatIndex = (int)((startLatTogether - endLat)/(0.01 * gid_level));

        int endLatIndex = (int)CalculateUtil.divide(CalculateUtil.subtract(startLatTogether, startLat), step_length * gid_level, 2);
        int startLatIndex = (int)CalculateUtil.divide(CalculateUtil.subtract(startLatTogether, endLat), step_length * gid_level, 2);
        List<List<Float>> splitLat = row_all.subList(startLatIndex, endLatIndex+1);

//        int startLonIndex = (int)((startLon - startLonTogether)/(0.01 * gid_level));
//        int endLonIndex = (int)((endLon - startLonTogether)/(0.01 * gid_level));
        int startLonIndex = (int)CalculateUtil.divide(CalculateUtil.subtract(startLon, startLonTogether), step_length * gid_level, 2);
        int endLonIndex = (int)CalculateUtil.divide(CalculateUtil.subtract(endLon, startLonTogether), step_length * gid_level, 2);

        List<List<Float>> splitLon = new ArrayList();
        for(List<Float> li:splitLat) {
            splitLon.add(li.subList(startLonIndex, endLonIndex+1));
        }
        long finalSplitTime = System.currentTimeMillis() - chouxiTimeStart;
        System.out.println("从抽希过的二维数组中获取最终的二维数组时间：" + finalSplitTime);

        map_return.put("查询+抽希时间：", selectAndExtraction + "ms");
        map_return.put("抽希完的矩阵合并时间：", hebingTime+"ms");
        map_return.put("从抽希后的大二维数组截取目标二维数组时间：", finalSplitTime + "ms");
        map_return.put("最终纬度点数：", splitLon.size());
        map_return.put("最终经度点数：", splitLon.get(0).size());
        for(Map.Entry<Integer, Long> m:selectTimeMap.entrySet()) {
            System.out.println("查询Hbase时间："+m.getValue()+"ms");
        }
        for(Map.Entry<Integer, Long> m:chouxiTimeMap.entrySet()) {
            System.out.println("抽希时间："+m.getValue()+"ms");
        }
        return map_return;
    }

    /**
     * 多线程嵌套多线程
     * 从Hbase查询，直接转为一维float数组，然后转为二维float数组
     * 先将目标矩阵块查出来进行抽希，然后按条件进行截取，多线程
     * @param gid_level
     * @param startLat
     * @param endLat
     * @param startLon
     * @param endLon
     * @return
     */
    public Map<String, Object> statisticalQueryTest4(double step_length, Integer gid_level, Integer thread_num, Integer son_thread_num, Double startLat,
                                                     Double endLat, Double startLon, Double endLon) {
        Map<String, Object> map_return = new HashMap<>();
        String rowkey_prefix = "2018092903_0_";
        int split_interval = (int)(10.0/step_length) + 1;//计算每行有多少格点数
        //计算开始和结束纬度之间，需要在hbase中查询的所有rowkey的纬度拼接部分
        List<String> rowkey_lat = RowKeyUtil.getRowKeyByLonOrLat(startLat,endLat);
        //计算开始和结束经度之间，需要在hbase中查询的所有rowkey的经度拼接部分
        List<String> rowkey_lon = RowKeyUtil.getRowKeyByLonOrLat(startLon,endLon);
        Collections.reverse(rowkey_lon);
        //从Hbase查询出的所有矩阵，抽希完之后的经纬度开始点
        double startLatTogether = Double.parseDouble(rowkey_lat.get(0).split("_")[1]);
        double startLonTogether = Double.parseDouble(rowkey_lon.get(0).split("_")[0]);;

        List<List<Float>> row_all = new ArrayList<>(); //每个纬度范围的数据块拼成最终的一个list，进行抽希
        TreeMap<Integer, TreeMap<Integer,List<List<Float>>>> row_all_map_map=new TreeMap<>();
        Connection conn = null;
        Table table = null;
        Long selectTime = 0l;
        ExecutorService executor = Executors.newFixedThreadPool(thread_num);
        Map<Integer,Map<Integer, Long>> selectTimeMapMap = new HashMap<>();//记录所有线程的查询时间
        Map<Integer,Map<Integer, Long>> chouxiTimeMapMap = new HashMap<>();//记录所有线程的抽希时间

        try {
            conn = ConnectionFactory.createConnection(hbaseTemplate.getConfiguration());
            TableName tableName = TableName.valueOf("weather_2018092903");
            table = conn.getTable(tableName);
            selectTime = System.currentTimeMillis();
            for(int i = 0; i < rowkey_lat.size(); i++) {
                final int i_run = i;
                final Table table_run = table;
                executor.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            Map<Integer, Long> selectTimeMap = new HashMap<>();//一个纬度范围内，所有经度范围的矩阵
                            Map<Integer, Long> chouxiTimeMap = new HashMap<>();//一个纬度范围内，所有经度范围的矩阵
                            TreeMap<Integer,List<List<Float>>> row_all_map=new TreeMap<>();//一个纬度范围内，所有经度范围的矩阵
                            String rowkey_lat_str = rowkey_lat.get(i_run);//获取一个rowkey中纬度的范围
                            List<Get> getList = new ArrayList<>();
                            for(String rowkey_lon_str:rowkey_lon) {
                                Get get = new Get(Bytes.toBytes(rowkey_prefix+rowkey_lat_str+"_"+rowkey_lon_str + "_" + step_length));
                                get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("Himawari8"));
                                getList.add(get);
                            }
                            ExecutorService service = Executors.newFixedThreadPool(son_thread_num);
                            //同一个纬度范围内，遍历所有经度范围的矩阵块
                            for (int j = 0; j < getList.size(); j++) {
                                final int j_run = j;
                                service.submit(new Runnable() {
                                    @Override
                                    public void run() {
                                        try {
                                            long selectTime = 0l;//当前线程中，查询Hbase时间
                                            long chouxiTime = 0l;//当前线程中，抽希的时间
                                            List<List<Float>> temp_row = new ArrayList();//Hbase中存储的每一块原始矩阵数据
                                            List<List<Float>> temp_row_clean = new ArrayList<>();//Hbase中存储的每一块抽希后的矩阵数据
                                            long selectTimeStart = System.currentTimeMillis();
                                            Result result = table_run.get(getList.get(j_run));//从Hbase查询
                                            selectTime += System.currentTimeMillis() - selectTimeStart;

                                            long chouxiTimeStart = System.currentTimeMillis();
                                            float[] arr_float = ConversionUtil.bytesToFloat(result.value());//将Hbase中的字节类型转为float数组
                                            int split_num = arr_float.length / split_interval;
                                            for(int s = 0; s < split_num; s++) {
                                                List<Float> l_temp = new ArrayList<>();
                                                for(int ss=s*split_interval; ss < (s+1)*split_interval; ss++) {
                                                    l_temp.add(arr_float[ss]);
                                                }
                                                temp_row.add(l_temp);
                                            }
                                            //计算抽希需要循环多少次
                                            int cycle_number = temp_row.size()/gid_level;
                                            //行数和抽希级别，是否是整数倍关系。
                                            int yushu = temp_row.size()%gid_level;
                                            //开始抽希
                                            for(int n = 0; n < cycle_number; n++) {//遍历每一行
                                                List arr_temp1 = new ArrayList<>();
                                                for(int nn = 0; nn < cycle_number; nn++) {
                                                    arr_temp1.add(temp_row.get(n * gid_level).get(nn * gid_level));
                                                }
                                                if(j_run == getList.size() - 1 && yushu != 0) {//同一纬度范围，当前是最后一个经度范围的时候
                                                    arr_temp1.add(temp_row.get(n*gid_level).get(temp_row.size() - 1));
                                                }
                                                temp_row_clean.add(arr_temp1);
                                            }
                                            if(i_run == rowkey_lat.size() - 1 && yushu != 0) {//如果是最后一个纬度范围，所有经度范围抽希结果需要加上最后一行
                                                List ll = new ArrayList();//存储最后一行抽希结果
                                                for(int n = 0; n < cycle_number; n++) {
                                                    ll.add(temp_row.get(temp_row.size() - 1).get(n * gid_level));
                                                }
                                                ll.add(temp_row.get(temp_row.size() - 1).get(temp_row.size() - 1));
                                                temp_row_clean.add(ll);
                                            }
                                            chouxiTime += System.currentTimeMillis() - chouxiTimeStart;
                                            selectTimeMap.put(j_run, selectTime);
                                            chouxiTimeMap.put(j_run, chouxiTime);
                                            row_all_map.put(j_run, temp_row_clean);
                                        }catch(IOException ex) {
                                            ex.printStackTrace();
                                        }

                                    }
                                });
                            }
                            service.shutdown();
                            while(true){
                                if(service.isTerminated()){
                                    System.out.println("-----所有的子线程都结束了！");
                                    break;
                                }
                            }
                            selectTimeMapMap.put(i_run, selectTimeMap);
                            chouxiTimeMapMap.put(i_run, chouxiTimeMap);
                            row_all_map_map.put(i_run, row_all_map);
                        }catch(Exception ex) {
                            ex.printStackTrace();
                        }
                    }
                });

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        executor.shutdown();
        while(true){
            if(executor.isTerminated()){
                System.out.println("-----所有的子线程都结束了！");
                break;
            }
        }

        //多线程抽希完的矩阵合并
        long hebingTimeStart = System.currentTimeMillis();
        for(Map.Entry<Integer, TreeMap<Integer, List<List<Float>>>> m:row_all_map_map.entrySet()) {
            List<List<Float>> one_row = new ArrayList();
            int index = 0;
            for(Map.Entry<Integer, List<List<Float>>> mm:m.getValue().entrySet()) {
                if(index == 0) {
                    one_row.addAll(mm.getValue());
                }else {
                    for(int i = 0; i < one_row.size(); i++) {
                        one_row.get(i).addAll(mm.getValue().get(i));
                    }
                }
                index++;
            }
            row_all.addAll(one_row);
        }
        long hebingTime = System.currentTimeMillis() - hebingTimeStart;
        System.out.println("抽希完的矩阵合并时间："+hebingTime+"ms");
        long selectAndExtraction = System.currentTimeMillis() - selectTime;
        System.out.println("查询+抽希时间："+ selectAndExtraction);
        //从抽希完的二维数组中，截取查询结果
        long chouxiTimeStart = System.currentTimeMillis();

        int endLatIndex = (int)CalculateUtil.divide(CalculateUtil.subtract(startLatTogether, startLat), step_length * gid_level, 2);
        int startLatIndex = (int)CalculateUtil.divide(CalculateUtil.subtract(startLatTogether, endLat), step_length * gid_level, 2);
        List<List<Float>> splitLat = row_all.subList(startLatIndex, endLatIndex+1);

        int startLonIndex = (int)CalculateUtil.divide(CalculateUtil.subtract(startLon, startLonTogether), step_length * gid_level, 2);
        int endLonIndex = (int)CalculateUtil.divide(CalculateUtil.subtract(endLon, startLonTogether), step_length * gid_level, 2);

        List<List<Float>> splitLon = new ArrayList();
        for(List<Float> li:splitLat) {
            splitLon.add(li.subList(startLonIndex, endLonIndex+1));
        }
        long finalSplitTime = System.currentTimeMillis() - chouxiTimeStart;
        System.out.println("从抽希过的二维数组中获取最终的二维数组时间：" + finalSplitTime);

        map_return.put("查询+抽希时间：", selectAndExtraction + "ms");
        map_return.put("抽希完的矩阵合并时间：", hebingTime+"ms");
        map_return.put("从抽希后的大二维数组截取目标二维数组时间：", finalSplitTime + "ms");
        map_return.put("最终纬度点数：", splitLon.size());
        map_return.put("最终经度点数：", splitLon.get(0).size());
        for(Map.Entry<Integer, Map<Integer, Long>> m:selectTimeMapMap.entrySet()) {
            for(Map.Entry<Integer, Long> mm:m.getValue().entrySet()) {
                System.out.println("查询Hbase时间："+mm.getValue()+"ms");
            }
        }
        for(Map.Entry<Integer, Map<Integer, Long>> m:chouxiTimeMapMap.entrySet()) {
            for(Map.Entry<Integer, Long> mm:m.getValue().entrySet()) {
                System.out.println("抽希时间："+mm.getValue()+"ms");
            }
        }

        return map_return;
    }

    /**
     * 多线程，每个线程取一块rowkey的名为Himawari8的列
     * 不写入文件，不合并
     * 不抽希
     * @param startLat
     * @param endLat
     * @param startLon
     * @param endLon
     * @return
     */
    public Map<String, Object> statisticalQueryTest5(double step_length, Integer thread_num, Double startLat,
                                                     Double endLat, Double startLon, Double endLon, String table_name) {
        Map<String, Object> map_return = new HashMap<>();
        //生成的csv文件列表的父目录
        String father_dir = CSVPath + DateUtil.formatDate(new Date(), "yyyyMMddHHmmss") + "/";
        String rowkey_prefix = "2018092903_0_";
        int split_interval = (int)(10.0/step_length) + 1;//计算每行，每列有多少格点数
        //计算开始和结束纬度之间，需要在hbase中查询的所有rowkey的纬度拼接部分，升序
        List<String> rowkey_lat = RowKeyUtil.getRowKeyByLonOrLat(startLat,endLat);
        //计算开始和结束经度之间，需要在hbase中查询的所有rowkey的经度拼接部分，升序
        List<String> rowkey_lon = RowKeyUtil.getRowKeyByLonOrLat(startLon,endLon);
        Collections.reverse(rowkey_lon);//纬度范围变为降序
        //第一个纬度范围，开始的纬度坐标
        double start_lat = Double.parseDouble(rowkey_lat.get(0).split("_")[1]);
        //最后一个纬度范围，开始的纬度坐标
        double end_lat = Double.parseDouble(rowkey_lat.get(rowkey_lat.size() - 1).split("_")[1]);
        //第一个经度范围，开始的经度坐标，开始索引
        double start_lon = Double.parseDouble(rowkey_lon.get(0).split("_")[0]);
        //最后一个经度范围，开始的经度坐标，开始索引
        double end_lon = Double.parseDouble(rowkey_lon.get(rowkey_lon.size() - 1).split("_")[0]);

//        Connection conn = null;
//        Table table = null;
//        Long select_time_start = 0l;//统计查询时间-开始时间
//        long select_time_total = 0l;//统计查询时间-总时间
        long final_time_start = System.currentTimeMillis();//统计总时间-开始时间
        long final_time_total = 0l;//统计总时间-总时间
        Map<String, Long> map_all_select_time = new HashMap<>();//每个线程分别查询的时间
        ExecutorService service = Executors.newFixedThreadPool(thread_num);
//        try {
//            conn = ConnectionFactory.createConnection(hbaseTemplate.getConfiguration());
//            TableName tableName = TableName.valueOf("weather_2018092903");
//            table = conn.getTable(tableName,Executors.newFixedThreadPool(thread_num));
//
//            List<Get> getList = new ArrayList<>();
//            for(String rowkey_lat_str:rowkey_lat) {
//                for(String rowkey_lon_str:rowkey_lon) {
//                    Get get = new Get(Bytes.toBytes(rowkey_prefix+rowkey_lat_str+"_"+rowkey_lon_str + "_" + step_length));
//                    get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("Himawari8"));
//                    getList.add(get);
//                }
//            }
//            select_time_start = System.currentTimeMillis();
//            Result[] data = new Result[getList.size()];
//            table.batch(getList, data);
//            select_time_total = System.currentTimeMillis() - select_time_start;
            for(int i = 0; i < rowkey_lat.size(); i++) {
                final int i_run = i;
                //计算，在该纬度范围内的所有矩阵，纬度的开始索引和结束索引
                int start_lat_index = 0;
                int end_lat_index = split_interval - 1;
                if(rowkey_lat.size() == 1) {
                    end_lat_index = (int)CalculateUtil.divide(CalculateUtil.subtract(start_lat, startLat), step_length, 2);
                    start_lat_index = (int)CalculateUtil.divide(CalculateUtil.subtract(end_lat, endLat), step_length, 2);
                }else {
                    if(i_run == 0) {
                        start_lat_index = (int)CalculateUtil.divide(CalculateUtil.subtract(start_lat, endLat), step_length, 2);
                    }else if(i_run == rowkey_lat.size() - 1) {
                        end_lat_index = (int)CalculateUtil.divide(CalculateUtil.subtract(end_lat, startLat), step_length, 2);
                    }
                }
                final int final_end_lat_index = end_lat_index;
                final int final_start_lat_index = start_lat_index;
                //同一个纬度范围内，遍历所有经度范围的矩阵块
                for (int j = 0; j < rowkey_lon.size(); j++) {
                    final int j_run = j;
                    int start_lon_index = 0;
                    int end_lon_index = split_interval - 1;
                    if(rowkey_lon.size() == 1) {
                        start_lon_index = (int)CalculateUtil.divide(CalculateUtil.subtract(startLon, start_lon), step_length, 2);
                        end_lon_index = (int)CalculateUtil.divide(CalculateUtil.subtract(endLon, end_lon), step_length, 2);
                    }else {
                        if(j_run == 0) {
                            start_lon_index = (int)CalculateUtil.divide(CalculateUtil.subtract(start_lon, startLon), step_length, 2);
                        }else if(j_run == rowkey_lon.size() - 1) {
                            end_lon_index = (int)CalculateUtil.divide(CalculateUtil.subtract(end_lon, endLon), step_length, 2);
                        }
                    }
                    final int final_start_lon_index = start_lon_index;
                    final int final_end_lon_index = end_lon_index;
                    service.submit(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                Connection conn = ConnectionFactory.createConnection(hbaseTemplate.getConfiguration());
                                TableName tableName = TableName.valueOf(table_name);
                                Table table = conn.getTable(tableName,Executors.newFixedThreadPool(thread_num));

                                List<Get> getList = new ArrayList<>();
                                Get get = new Get(Bytes.toBytes(rowkey_prefix+rowkey_lat.get(i_run)+"_"+rowkey_lon.get(j_run) + "_" + step_length));
                                getList.add(get);
                                long select_time_start = System.currentTimeMillis();
                                Result[] data = new Result[getList.size()];
                                table.batch(getList, data);
                                long select_time_total = System.currentTimeMillis() - select_time_start;
                                map_all_select_time.put(i_run+"_"+j_run, select_time_total);
//                            Result result = data[i_run*rowkey_lon.size()+j_run];//从Hbase查询

                                int row_size = final_end_lat_index - final_start_lat_index + 1;//二维的float数组，第一维的size
                                int column_size = final_end_lon_index - final_start_lon_index + 1;//二维的float数组，第二维的size
                                float [][] float_arr2= new float[row_size][column_size];//Hbase查出来的结果转换成二维数组，需要写入文件
                                float[] arr_float = ConversionUtil.bytesToFloat(data[0].value());//将Hbase中的字节类型转为float数组
//                                  int split_num = arr_float.length / split_interval;//Hbase原始矩阵一共有多少行
                                int index = 0;
                                for(int s = final_start_lat_index; s <= final_end_lat_index; s++) {
                                    float[] l_temp = new float[column_size];
                                    System.arraycopy(arr_float, split_interval * s + final_start_lon_index, l_temp, 0, column_size);
                                    float_arr2[index] = l_temp;
                                    index++;
                                }
                                String path = father_dir+i_run+"_"+j_run+".csv";
//                                            CSVUtil.FloatArray2Csv(float_arr2, path);
                                table.close();
                                conn.close();
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                    });
                }
            }
        service.shutdown();
        while(true){
            if(service.isTerminated()){
                System.out.println("一个子线程结束");
                break;
            }
        }
        final_time_total = System.currentTimeMillis() - final_time_start;

        map_return.put("total_time", final_time_total + "ms");
        long select_time_total = 0l;
        for(Map.Entry<String, Long> m:map_all_select_time.entrySet()) {
            select_time_total += m.getValue();
            System.out.println("查询时间：" + m.getValue());
        }
//        map_return.put("select_time", select_time_total + "ms");
        return map_return;
    }

    /**
     * 多线程嵌套多线程，写入本地csv文件
     * 从Hbase查询，直接转为一维float数组，然后转为二维float数组
     * 抽希
     * @param startLat
     * @param endLat
     * @param startLon
     * @param endLon
     * @return
     */
    public Map<String, Object> statisticalQueryTest6(double step_length, int gid_level, Integer thread_num, Double startLat,
                                                     Double endLat, Double startLon, Double endLon) {
        Map<String, Object> map_return = new HashMap<>();
        //生成的csv文件列表的父目录
        String father_dir = CSVPath + DateUtil.formatDate(new Date(), "yyyyMMddHHmmss") + "/";
        String rowkey_prefix = "2018092903_0_";
        int split_interval = (int)(10.0/step_length) + 1;//计算每行，每列有多少格点数
        int split_interval_clean = (int)(10.0/(step_length * gid_level)) + 1;////计算抽希后，每行，每列有多少格点数
        //计算开始和结束纬度之间，需要在hbase中查询的所有rowkey的纬度拼接部分，升序
        List<String> rowkey_lat = RowKeyUtil.getRowKeyByLonOrLat(startLat,endLat);
        //计算开始和结束经度之间，需要在hbase中查询的所有rowkey的经度拼接部分，升序
        List<String> rowkey_lon = RowKeyUtil.getRowKeyByLonOrLat(startLon,endLon);
        Collections.reverse(rowkey_lon);//纬度范围变为降序
        //第一个纬度范围，开始的纬度坐标
        double start_lat = Double.parseDouble(rowkey_lat.get(0).split("_")[1]);
        //最后一个纬度范围，开始的纬度坐标
        double end_lat = Double.parseDouble(rowkey_lat.get(rowkey_lat.size() - 1).split("_")[1]);
        //第一个经度范围，开始的经度坐标，开始索引
        double start_lon = Double.parseDouble(rowkey_lon.get(0).split("_")[0]);
        //最后一个经度范围，开始的经度坐标，开始索引
        double end_lon = Double.parseDouble(rowkey_lon.get(rowkey_lon.size() - 1).split("_")[0]);

        Connection conn = null;
        Table table = null;
        long final_time_start = System.currentTimeMillis();//计算总时间-开始时间
        long final_time_total = 0l;//计算总时间-总时间
        long select_time_start = 0l;//计算查询时间-开始时间
        long select_time_total = 0l;//计算查询时间-总时间
        long chouxi_time_start = 0l;//计算抽希时间-开始时间
        long chouxi_time_total = 0l;//计算抽希时间-总时间
        ExecutorService service = Executors.newFixedThreadPool(thread_num);//线程池

        try {
            conn = ConnectionFactory.createConnection(hbaseTemplate.getConfiguration());
            TableName tableName = TableName.valueOf("weather_2018092903");
            table = conn.getTable(tableName,Executors.newFixedThreadPool(thread_num));
            List<Get> getList = new ArrayList<>();
            for(String rowkey_lat_str:rowkey_lat) {
                for(String rowkey_lon_str:rowkey_lon) {
                    Get get = new Get(Bytes.toBytes(rowkey_prefix+rowkey_lat_str+"_"+rowkey_lon_str + "_" + step_length));
                    get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("Himawari8"));
                    getList.add(get);
                }
            }
            select_time_start = System.currentTimeMillis();//查询开始时间
            Result[] data = new Result[getList.size()];
            table.batch(getList, data);
            select_time_total = System.currentTimeMillis() - select_time_start;//查询总时间

            chouxi_time_start = System.currentTimeMillis();//抽希开始时间
            for(int i = 0; i < rowkey_lat.size(); i++) {
                final int i_run = i;
                //计算，在该纬度范围内的所有矩阵，纬度的开始索引和结束索引
                int start_lat_index = 0;
                int end_lat_index = split_interval_clean - 1;
                if(rowkey_lat.size() == 1) {
                    end_lat_index = (int)CalculateUtil.divide(CalculateUtil.subtract(start_lat, startLat), step_length * gid_level, 2);
                    start_lat_index = (int)CalculateUtil.divide(CalculateUtil.subtract(end_lat, endLat), step_length * gid_level, 2);
                }else {
                    if(i_run == 0) {
                        start_lat_index = (int)CalculateUtil.divide(CalculateUtil.subtract(start_lat, endLat), step_length * gid_level, 2);
                    }else if(i_run == rowkey_lat.size() - 1) {
                        end_lat_index = (int)CalculateUtil.divide(CalculateUtil.subtract(end_lat, startLat), step_length * gid_level, 2);
                    }
                }
                final int final_end_lat_index = end_lat_index;
                final int final_start_lat_index = start_lat_index;
                //同一个纬度范围内，遍历所有经度范围的矩阵块
                for (int j = 0; j < rowkey_lon.size(); j++) {
                    final int j_run = j;
                    int start_lon_index = 0;
                    int end_lon_index = split_interval_clean - 1;
                    if(rowkey_lon.size() == 1) {
                        start_lon_index = (int)CalculateUtil.divide(CalculateUtil.subtract(startLon, start_lon), step_length * gid_level, 2);
                        end_lon_index = (int)CalculateUtil.divide(CalculateUtil.subtract(endLon, end_lon), step_length * gid_level, 2);
                    }else {
                        if(j_run == 0) {
                            start_lon_index = (int)CalculateUtil.divide(CalculateUtil.subtract(start_lon, startLon), step_length * gid_level, 2);
                        }else if(j_run == getList.size() - 1) {
                            end_lon_index = (int)CalculateUtil.divide(CalculateUtil.subtract(end_lon, endLon), step_length * gid_level, 2);
                        }
                    }
                    final int final_start_lon_index = start_lon_index;
                    final int final_end_lon_index = end_lon_index;
                    service.submit(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                Result result = data[i_run*rowkey_lon.size()+j_run];//从Hbase查询
//                                System.out.println("Result[]的当前索引：" + (i_run*rowkey_lon.size()+j_run));
                                int row_size = final_end_lat_index - final_start_lat_index + 1;//二维的float数组，第一维的size
                                int column_size = final_end_lon_index - final_start_lon_index + 1;//二维的float数组，第二维的size

                                float [][] float_arr2= new float[row_size][column_size];//Hbase查出来的结果转换成二维数组，需要写入文件
                                float[] arr_float = ConversionUtil.bytesToFloat(result.value());//将Hbase中的字节类型转为float数组
                                int split_num = arr_float.length / split_interval;//Hbase原始矩阵一共有多少行
                                int index = 0;
                                int l_temp_index = 0;
                                for(int s = 0; s < split_num; s++) {
                                    if(s%gid_level == 0) {
                                        if(index >= final_start_lat_index && index <= final_end_lat_index) {
                                            float[] l_temp = new float[split_interval];
                                            System.arraycopy(arr_float, split_interval * s, l_temp, 0, split_interval);
                                            int column_index = 0;
                                            int l_temp_final_index = 0;
                                            float[] l_temp_final = new float[column_size];
                                            for(int c = 0; c < l_temp.length; c++) {
                                                if(c%gid_level == 0) {
                                                    if(column_index >= final_start_lon_index && column_index <= final_end_lon_index) {
                                                        l_temp_final[l_temp_final_index] = l_temp[c];
                                                        l_temp_final_index++;
                                                    }
                                                    column_index++;
                                                }
                                            }
                                            float_arr2[l_temp_index] = l_temp_final;
                                            l_temp_index++;
                                        }
                                        index++;
                                    }
                                }
                                String path = father_dir+i_run+"_"+j_run+".csv";
//                                            CSVUtil.FloatArray2Csv(float_arr2, path);

                            }catch(Exception ex) {
                                ex.printStackTrace();
                            }

                        }
                    });
                }

            }
        } catch (InterruptedException e1) {
            e1.printStackTrace();
        } catch (IOException e) {
        e.printStackTrace();
        }
        service.shutdown();
        while(true){
            if(service.isTerminated()){
                System.out.println("一个子线程结束");
                break;
            }
        }
        chouxi_time_total = System.currentTimeMillis() - chouxi_time_start;//计算抽希总时长
        try {
            table.close();
            conn.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        final_time_total = System.currentTimeMillis() - final_time_start;
        map_return.put("总时间：", final_time_total + "ms");
        map_return.put("查询消耗时间：", select_time_total + "ms");
        map_return.put("抽希消耗时间：", chouxi_time_total + "ms");
        return map_return;
    }

    /**
     * hbaseTemplate，用rowkey批量查询
     * @param step_length
     * @param gid_level
     * @param thread_num
     * @param son_thread_num
     * @param startLat
     * @param endLat
     * @param startLon
     * @param endLon
     * @return
     */
    public Map<String, Object> statisticalQueryTest7(double step_length, int gid_level, Integer thread_num, Integer son_thread_num, Double startLat,
                                                     Double endLat, Double startLon, Double endLon) {
        long final_time_start = System.currentTimeMillis();//统计全部用时-开始时刻
        Map<String, Object> map_return = new HashMap<>();
        //生成的csv文件列表的父目录
        String father_dir = CSVPath + DateUtil.formatDate(new Date(), "yyyyMMddHHmmss") + "/";
        String rowkey_prefix = "2018092903_0_";
        int split_interval = (int)(10.0/step_length) + 1;//计算每行，每列有多少格点数
        int split_interval_clean = (int)(10.0/(step_length * gid_level)) + 1;////计算抽希后，每行，每列有多少格点数
        //计算开始和结束纬度之间，需要在hbase中查询的所有rowkey的纬度拼接部分，升序
        List<String> rowkey_lat = RowKeyUtil.getRowKeyByLonOrLat(startLat,endLat);
        //计算开始和结束经度之间，需要在hbase中查询的所有rowkey的经度拼接部分，升序
        List<String> rowkey_lon = RowKeyUtil.getRowKeyByLonOrLat(startLon,endLon);
        Collections.reverse(rowkey_lon);//纬度范围变为降序
        //第一个纬度范围，开始的纬度坐标
        double start_lat = Double.parseDouble(rowkey_lat.get(0).split("_")[1]);
        //最后一个纬度范围，开始的纬度坐标
        double end_lat = Double.parseDouble(rowkey_lat.get(rowkey_lat.size() - 1).split("_")[1]);
        //第一个经度范围，开始的经度坐标，开始索引
        double start_lon = Double.parseDouble(rowkey_lon.get(0).split("_")[0]);
        //最后一个经度范围，开始的经度坐标，开始索引
        double end_lon = Double.parseDouble(rowkey_lon.get(rowkey_lon.size() - 1).split("_")[0]);
        List<Get> getList = new ArrayList<>();
        for(String rowkey_lat_str:rowkey_lat) {
            for(String rowkey_lon_str:rowkey_lon) {
                Get get = new Get(Bytes.toBytes(rowkey_prefix+rowkey_lat_str+"_"+rowkey_lon_str + "_" + step_length));
                get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("Himawari8"));
                getList.add(get);
            }
        }
        hbaseTemplate.execute("weather_2018092903", new TableCallback<String>() {
            @Override
            public String doInTable(HTableInterface table) throws Throwable {
                Result[] data = table.get(getList);
                ExecutorService executor = Executors.newFixedThreadPool(thread_num);
                for(int i = 0; i < rowkey_lat.size(); i++) {
                    final int i_run = i;
                    //计算，在该纬度范围内的所有矩阵，纬度的开始索引和结束索引
                    int start_lat_index = 0;
                    int end_lat_index = split_interval_clean - 1;
                    if(rowkey_lat.size() == 1) {
                        end_lat_index = (int)CalculateUtil.divide(CalculateUtil.subtract(start_lat, startLat), step_length * gid_level, 2);
                        start_lat_index = (int)CalculateUtil.divide(CalculateUtil.subtract(end_lat, endLat), step_length * gid_level, 2);
                    }else {
                        if(i_run == 0) {
                            start_lat_index = (int)CalculateUtil.divide(CalculateUtil.subtract(start_lat, endLat), step_length * gid_level, 2);
                        }else if(i_run == rowkey_lat.size() - 1) {
                            end_lat_index = (int)CalculateUtil.divide(CalculateUtil.subtract(end_lat, startLat), step_length * gid_level, 2);
                        }
                    }
                    final int final_end_lat_index = end_lat_index;
                    final int final_start_lat_index = start_lat_index;
                    executor.submit(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                String rowkey_lat_str = rowkey_lat.get(i_run);//获取一个rowkey中纬度的范围
                                ExecutorService service = Executors.newFixedThreadPool(son_thread_num);
                                //同一个纬度范围内，遍历所有经度范围的矩阵块
                                for (int j = 0; j < rowkey_lon.size(); j++) {
                                    final int j_run = j;
                                    int start_lon_index = 0;
                                    int end_lon_index = split_interval_clean - 1;
                                    if(rowkey_lon.size() == 1) {
                                        start_lon_index = (int)CalculateUtil.divide(CalculateUtil.subtract(startLon, start_lon), step_length * gid_level, 2);
                                        end_lon_index = (int)CalculateUtil.divide(CalculateUtil.subtract(endLon, end_lon), step_length * gid_level, 2);
                                    }else {
                                        if(j_run == 0) {
                                            start_lon_index = (int)CalculateUtil.divide(CalculateUtil.subtract(start_lon, startLon), step_length * gid_level, 2);
                                        }else if(j_run == getList.size() - 1) {
                                            end_lon_index = (int)CalculateUtil.divide(CalculateUtil.subtract(end_lon, endLon), step_length * gid_level, 2);
                                        }
                                    }
                                    final int final_start_lon_index = start_lon_index;
                                    final int final_end_lon_index = end_lon_index;
                                    service.submit(new Runnable() {
                                        @Override
                                        public void run() {
                                            try {
                                                Result result = data[i_run*rowkey_lon.size()+j_run];//从Hbase查询

                                                int row_size = final_end_lat_index - final_start_lat_index + 1;//二维的float数组，第一维的size
                                                int column_size = final_end_lon_index - final_start_lon_index + 1;//二维的float数组，第二维的size

                                                float [][] float_arr2= new float[row_size][column_size];//Hbase查出来的结果转换成二维数组，需要写入文件
                                                float[] arr_float = ConversionUtil.bytesToFloat(result.value());//将Hbase中的字节类型转为float数组
                                                int split_num = arr_float.length / split_interval;//Hbase原始矩阵一共有多少行
                                                int index = 0;
                                                int l_temp_index = 0;
                                                for(int s = 0; s < split_num; s++) {
                                                    if(s%gid_level == 0) {
                                                        if(index >= final_start_lat_index && index <= final_end_lat_index) {
                                                            float[] l_temp = new float[split_interval];
                                                            System.arraycopy(arr_float, split_interval * s, l_temp, 0, split_interval);
                                                            int column_index = 0;
                                                            int l_temp_final_index = 0;
                                                            float[] l_temp_final = new float[column_size];
                                                            for(int c = 0; c < l_temp.length; c++) {
                                                                if(c%gid_level == 0) {
                                                                    if(column_index >= final_start_lon_index && column_index <= final_end_lon_index) {
                                                                        l_temp_final[l_temp_final_index] = l_temp[c];
                                                                        l_temp_final_index++;
                                                                    }
                                                                    column_index++;
                                                                }
                                                            }
                                                            float_arr2[l_temp_index] = l_temp_final;
                                                            l_temp_index++;
                                                        }
                                                        index++;
                                                    }
                                                }
                                                String path = father_dir+i_run+"_"+j_run+".csv";
//                                            CSVUtil.FloatArray2Csv(float_arr2, path);

                                            }catch(Exception ex) {
                                                ex.printStackTrace();
                                            }

                                        }
                                    });
                                }
                                service.shutdown();
                                while(true){
                                    if(service.isTerminated()){
                                        System.out.println("一个子线程结束");
                                        break;
                                    }
                                }
                            }catch(Exception ex) {
                                ex.printStackTrace();
                            }
                        }
                    });

                }
                executor.shutdown();
                while(true){
                    if(executor.isTerminated()){
                        System.out.println("-----所有的线程都结束了！");
                        break;
                    }
                }
                return "";
            }
        });

//        executor.shutdown();
//        while(true){
//            if(executor.isTerminated()){
//                System.out.println("-----所有的线程都结束了！");
//                break;
//            }
//        }

        long selectAndExtraction = System.currentTimeMillis() - final_time_start;
        System.out.println("最终耗费时间："+ selectAndExtraction + "ms");

        map_return.put("最终耗费时间：", selectAndExtraction + "ms");

        return map_return;
    }

    public Map<String, Object> statisticalQueryTest8(double step_length, int gid_level, Integer thread_num, Double startLat,
                                                     Double endLat, Double startLon, Double endLon) {
        Map<String, Object> map_return = new HashMap<>();
        String rowkey_prefix = "2018092903_0_";
        int split_interval = (int)(10.0/step_length) + 1;//计算每行，每列有多少格点数
        int split_interval_clean = (int)(10.0/(step_length * gid_level)) + 1;////计算抽希后，每行，每列有多少格点数
        //计算开始和结束纬度之间，需要在hbase中查询的所有rowkey的纬度拼接部分，降序
        List<String> rowkey_lat = RowKeyUtil.getRowKeyByLonOrLat(startLat,endLat);
        //计算开始和结束经度之间，需要在hbase中查询的所有rowkey的经度拼接部分，降序
        List<String> rowkey_lon = RowKeyUtil.getRowKeyByLonOrLat(startLon,endLon);
        Collections.reverse(rowkey_lon);//经度范围变为升序
        //第一个纬度范围，开始的纬度坐标
        double start_lat = Double.parseDouble(rowkey_lat.get(0).split("_")[1]);
        //最后一个纬度范围，开始的纬度坐标
        double end_lat = Double.parseDouble(rowkey_lat.get(rowkey_lat.size() - 1).split("_")[1]);
        //第一个经度范围，开始的经度坐标，开始索引
        double start_lon = Double.parseDouble(rowkey_lon.get(0).split("_")[0]);
        //最后一个经度范围，开始的经度坐标，开始索引
        double end_lon = Double.parseDouble(rowkey_lon.get(rowkey_lon.size() - 1).split("_")[0]);
        Connection conn = null;
        Table table = null;
        Long final_time_start = System.currentTimeMillis();//统计总时间-开始时间
        long select_time_start = 0l;//统计查询时间-开始时间
        long chouxi_time_start = 0l;//统计抽希时间-开始时间
        List<Get> getList = new ArrayList<>();
        ExecutorService service = Executors.newFixedThreadPool(thread_num);//线程池
        try {
            conn = ConnectionFactory.createConnection(hbaseTemplate.getConfiguration());
            TableName tableName = TableName.valueOf("weather_2018092903");
            table = conn.getTable(tableName,Executors.newFixedThreadPool(thread_num));
            for(String rowkey_lat_str : rowkey_lat) {
                for (String rowkey_lon_str : rowkey_lon) {
                    Get get = new Get(Bytes.toBytes(rowkey_prefix + rowkey_lat_str + "_" + rowkey_lon_str + "_" + 0.01));
                    get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("Himawari8"));
                    getList.add(get);
                }
            }
            select_time_start = System.currentTimeMillis();
            Result[] data = new Result[getList.size()];
            table.batch(getList, data);
            map_return.put("查询时间：", System.currentTimeMillis() - select_time_start);
            chouxi_time_start = System.currentTimeMillis();
            for(int i = 0; i < rowkey_lat.size(); i++) {
                final int i_run = i;
                //计算，在该纬度范围内的所有矩阵，纬度的开始索引和结束索引
                int start_lat_index = 0;
                int end_lat_index = split_interval_clean - 1;
                if (rowkey_lat.size() == 1) {
                    end_lat_index = (int) CalculateUtil.divide(CalculateUtil.subtract(start_lat, startLat), step_length * gid_level, 2);
                    start_lat_index = (int) CalculateUtil.divide(CalculateUtil.subtract(end_lat, endLat), step_length * gid_level, 2);
                } else {
                    if (i_run == 0) {
                        start_lat_index = (int) CalculateUtil.divide(CalculateUtil.subtract(start_lat, endLat), step_length * gid_level, 2);
                    } else if (i_run == rowkey_lat.size() - 1) {
                        end_lat_index = (int) CalculateUtil.divide(CalculateUtil.subtract(end_lat, startLat), step_length * gid_level, 2);
                    }
                }
                final int final_end_lat_index = end_lat_index;
                final int final_start_lat_index = start_lat_index;
                //同一个纬度范围内，遍历所有经度范围的矩阵块
                for (int j = 0; j < rowkey_lon.size(); j++) {
                    final int j_run = j;
                    int start_lon_index = 0;
                    int end_lon_index = split_interval_clean - 1;
                    if(rowkey_lon.size() == 1) {
                        start_lon_index = (int)CalculateUtil.divide(CalculateUtil.subtract(startLon, start_lon), step_length * gid_level, 2);
                        end_lon_index = (int)CalculateUtil.divide(CalculateUtil.subtract(endLon, end_lon), step_length * gid_level, 2);
                    }else {
                        if(j_run == 0) {
                            start_lon_index = (int)CalculateUtil.divide(CalculateUtil.subtract(start_lon, startLon), step_length * gid_level, 2);
                        }else if(j_run == getList.size() - 1) {
                            end_lon_index = (int)CalculateUtil.divide(CalculateUtil.subtract(end_lon, endLon), step_length * gid_level, 2);
                        }
                    }
                    final int final_start_lon_index = start_lon_index;
                    final int final_end_lon_index = end_lon_index;
                    service.submit(new Runnable() {
                        @Override
                        public void run() {
                            Result result = data[i_run*rowkey_lon.size()+j_run];//从Hbase查询
//                                System.out.println("Result[]的当前索引：" + (i_run*rowkey_lon.size()+j_run));
                            int row_size = final_end_lat_index - final_start_lat_index + 1;//二维的float数组，第一维的size
                            int column_size = final_end_lon_index - final_start_lon_index + 1;//二维的float数组，第二维的size

                            float [][] float_arr2= new float[row_size][column_size];//Hbase查出来的结果转换成二维数组，需要写入文件
                            float[] arr_float = ConversionUtil.bytesToFloat(result.value());//将Hbase中的字节类型转为float数组
                            int split_num = arr_float.length / split_interval;//Hbase原始矩阵一共有多少行
                            int index = 0;
                            int l_temp_index = 0;
                            for(int s = 0; s < split_num; s++) {
                                if(s%gid_level == 0) {
                                    if(index >= final_start_lat_index && index <= final_end_lat_index) {
                                        float[] l_temp = new float[split_interval];
                                        System.arraycopy(arr_float, split_interval * s, l_temp, 0, split_interval);
                                        int column_index = 0;
                                        int l_temp_final_index = 0;
                                        float[] l_temp_final = new float[column_size];
                                        for(int c = 0; c < l_temp.length; c++) {
                                            if(c%gid_level == 0) {
                                                if(column_index >= final_start_lon_index && column_index <= final_end_lon_index) {
                                                    l_temp_final[l_temp_final_index] = l_temp[c];
                                                    l_temp_final_index++;
                                                }
                                                column_index++;
                                            }
                                        }
                                        float_arr2[l_temp_index] = l_temp_final;
                                        l_temp_index++;
                                    }
                                    index++;
                                }
                            }
//                            String path = father_dir+i_run+"_"+j_run+".csv";
//                            CSVUtil.FloatArray2Csv(float_arr2, path);
                        }
                    });
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        service.shutdown();
        while(true){
            if(service.isTerminated()){
                System.out.println("线程结束");
                break;
            }
        }
        map_return.put("抽希时间：", System.currentTimeMillis() - chouxi_time_start);

        //关闭Hbase表和连接
        try {
            table.close();
            conn.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        map_return.put("总时间", System.currentTimeMillis() - final_time_start);
        return map_return;
    }

    /**
     * 多线程，每个线程取一块rowkey的名为Himawari8的列
     * 不写入文件，不合并
     * 抽希
     * @param startLat
     * @param endLat
     * @param startLon
     * @param endLon
     * @return
     */
    public Map<String, Object> statisticalQueryTest9(double step_length, int gid_level, Integer thread_num, Double startLat,
                                                     Double endLat, Double startLon, Double endLon, String table_name) {
        Map<String, Object> map_return = new HashMap<>();
        //生成的csv文件列表的父目录
        String father_dir = CSVPath + DateUtil.formatDate(new Date(), "yyyyMMddHHmmss") + "/";
        String rowkey_prefix = "2018092903_0_";
        int split_interval = (int)(10.0/step_length) + 1;//计算每行，每列有多少格点数
        int split_interval_clean = (int)(10.0/(step_length * gid_level)) + 1;////计算抽希后，每行，每列有多少格点数
        //计算开始和结束纬度之间，需要在hbase中查询的所有rowkey的纬度拼接部分，升序
        List<String> rowkey_lat = RowKeyUtil.getRowKeyByLonOrLat(startLat,endLat);
        //计算开始和结束经度之间，需要在hbase中查询的所有rowkey的经度拼接部分，升序
        List<String> rowkey_lon = RowKeyUtil.getRowKeyByLonOrLat(startLon,endLon);
        Collections.reverse(rowkey_lon);//纬度范围变为降序
        //第一个纬度范围，开始的纬度坐标
        double start_lat = Double.parseDouble(rowkey_lat.get(0).split("_")[1]);
        //最后一个纬度范围，开始的纬度坐标
        double end_lat = Double.parseDouble(rowkey_lat.get(rowkey_lat.size() - 1).split("_")[1]);
        //第一个经度范围，开始的经度坐标，开始索引
        double start_lon = Double.parseDouble(rowkey_lon.get(0).split("_")[0]);
        //最后一个经度范围，开始的经度坐标，开始索引
        double end_lon = Double.parseDouble(rowkey_lon.get(rowkey_lon.size() - 1).split("_")[0]);

//        Connection conn = null;
//        Table table = null;
        Map<String, Long> map_all_select_time = new HashMap<>();//所有线程的查询时间
        long final_time_start = System.currentTimeMillis();//计算总时间-开始时间
        long final_time_total = 0l;//计算总时间-总时间
//        long select_time_start = 0l;//计算查询时间-开始时间
//        long select_time_total = 0l;//计算查询时间-总时间
//        long chouxi_time_start = 0l;//计算抽希时间-开始时间
//        long chouxi_time_total = 0l;//计算抽希时间-总时间
        ExecutorService service = Executors.newFixedThreadPool(thread_num);//线程池

//        try {
//            conn = ConnectionFactory.createConnection(hbaseTemplate.getConfiguration());
//            TableName tableName = TableName.valueOf("weather_2018092903");
//            table = conn.getTable(tableName,Executors.newFixedThreadPool(thread_num));
//            List<Get> getList = new ArrayList<>();
//            for(String rowkey_lat_str:rowkey_lat) {
//                for(String rowkey_lon_str:rowkey_lon) {
//                    Get get = new Get(Bytes.toBytes(rowkey_prefix+rowkey_lat_str+"_"+rowkey_lon_str + "_" + step_length));
//                    get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("Himawari8"));
//                    getList.add(get);
//                }
//            }
//            select_time_start = System.currentTimeMillis();//查询开始时间
//            Result[] data = new Result[getList.size()];
//            table.batch(getList, data);
//            select_time_total = System.currentTimeMillis() - select_time_start;//查询总时间

//            chouxi_time_start = System.currentTimeMillis();//抽希开始时间
            for(int i = 0; i < rowkey_lat.size(); i++) {
                final int i_run = i;
                //计算，在该纬度范围内的所有矩阵，纬度的开始索引和结束索引
                int start_lat_index = 0;
                int end_lat_index = split_interval_clean - 1;
                if(rowkey_lat.size() == 1) {
                    end_lat_index = (int)CalculateUtil.divide(CalculateUtil.subtract(start_lat, startLat), step_length * gid_level, 2);
                    start_lat_index = (int)CalculateUtil.divide(CalculateUtil.subtract(end_lat, endLat), step_length * gid_level, 2);
                }else {
                    if(i_run == 0) {
                        start_lat_index = (int)CalculateUtil.divide(CalculateUtil.subtract(start_lat, endLat), step_length * gid_level, 2);
                    }else if(i_run == rowkey_lat.size() - 1) {
                        end_lat_index = (int)CalculateUtil.divide(CalculateUtil.subtract(end_lat, startLat), step_length * gid_level, 2);
                    }
                }
                final int final_end_lat_index = end_lat_index;
                final int final_start_lat_index = start_lat_index;
                //同一个纬度范围内，遍历所有经度范围的矩阵块
                for (int j = 0; j < rowkey_lon.size(); j++) {
                    final int j_run = j;
                    int start_lon_index = 0;
                    int end_lon_index = split_interval_clean - 1;
                    if(rowkey_lon.size() == 1) {
                        start_lon_index = (int)CalculateUtil.divide(CalculateUtil.subtract(startLon, start_lon), step_length * gid_level, 2);
                        end_lon_index = (int)CalculateUtil.divide(CalculateUtil.subtract(endLon, end_lon), step_length * gid_level, 2);
                    }else {
                        if(j_run == 0) {
                            start_lon_index = (int)CalculateUtil.divide(CalculateUtil.subtract(start_lon, startLon), step_length * gid_level, 2);
                        }else if(j_run == rowkey_lon.size() - 1) {
                            end_lon_index = (int)CalculateUtil.divide(CalculateUtil.subtract(end_lon, endLon), step_length * gid_level, 2);
                        }
                    }
                    final int final_start_lon_index = start_lon_index;
                    final int final_end_lon_index = end_lon_index;
                    service.submit(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                Connection conn = ConnectionFactory.createConnection(hbaseTemplate.getConfiguration());
                                TableName tableName = TableName.valueOf(table_name);
                                Table table = conn.getTable(tableName,Executors.newFixedThreadPool(thread_num));
                                List<Get> getList = new ArrayList<>();
                                Get get = new Get(Bytes.toBytes(rowkey_prefix+rowkey_lat.get(i_run)+"_"+rowkey_lon.get(j_run) + "_" + step_length));
                                getList.add(get);
                                long select_time_start = System.currentTimeMillis();//查询开始时间
                                Result[] data = new Result[getList.size()];
                                table.batch(getList, data);
                                long select_time_total = System.currentTimeMillis() - select_time_start;//单个矩阵
                                map_all_select_time.put(i_run+"_"+"j_run", select_time_total);
//                                Result result = data[i_run*rowkey_lon.size()+j_run];//从Hbase查询
//                                System.out.println("Result[]的当前索引：" + (i_run*rowkey_lon.size()+j_run));
                                int row_size = final_end_lat_index - final_start_lat_index + 1;//二维的float数组，第一维的size
                                int column_size = final_end_lon_index - final_start_lon_index + 1;//二维的float数组，第二维的size

                                float [][] float_arr2= new float[row_size][column_size];//Hbase查出来的结果转换成二维数组，需要写入文件
                                float[] arr_float = ConversionUtil.bytesToFloat(data[0].value());//将Hbase中的字节类型转为float数组
                                int split_num = arr_float.length / split_interval;//Hbase原始矩阵一共有多少行
                                int index = 0;
                                int l_temp_index = 0;
                                for(int s = 0; s < split_num; s++) {
                                    if(s%gid_level == 0) {
                                        if(index >= final_start_lat_index && index <= final_end_lat_index) {
                                            float[] l_temp = new float[split_interval];
                                            System.arraycopy(arr_float, split_interval * s, l_temp, 0, split_interval);
                                            int column_index = 0;
                                            int l_temp_final_index = 0;
                                            float[] l_temp_final = new float[column_size];
                                            for(int c = 0; c < l_temp.length; c++) {
                                                if(c%gid_level == 0) {
                                                    if(column_index >= final_start_lon_index && column_index <= final_end_lon_index) {
                                                        l_temp_final[l_temp_final_index] = l_temp[c];
                                                        l_temp_final_index++;
                                                    }
                                                    column_index++;
                                                }
                                            }
                                            float_arr2[l_temp_index] = l_temp_final;
                                            l_temp_index++;
                                        }
                                        index++;
                                    }
                                }
                                String path = father_dir+i_run+"_"+j_run+".csv";
//                                            CSVUtil.FloatArray2Csv(float_arr2, path);
                                table.close();
                                conn.close();

                            }catch(Exception ex) {
                                ex.printStackTrace();
                            }

                        }
                    });
                }

            }
//        } catch (InterruptedException e1) {
//            e1.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        service.shutdown();
        while(true){
            if(service.isTerminated()){
                System.out.println("一个子线程结束");
                break;
            }
        }
//        chouxi_time_total = System.currentTimeMillis() - chouxi_time_start;//计算抽希总时长
//        try {
//            table.close();
//            conn.close();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        final_time_total = System.currentTimeMillis() - final_time_start;
        map_return.put("total_time", final_time_total + "ms");
        for(Map.Entry<String, Long> m:map_all_select_time.entrySet()) {
            System.out.println("查询时间："+m.getValue());
        }
//        map_return.put("查询消耗时间：", select_time_total + "ms");
//        map_return.put("抽希消耗时间：", chouxi_time_total + "ms");
        return map_return;
    }

    /**
     * 查询
     * 从存String的Hbase表中查询
     * 按最大值抽稀
     * @param step_length
     * @param gid_level
     * @param thread_num
     * @param startLat
     * @param endLat
     * @param startLon
     * @param endLon
     * @param table_name
     * @return
     */
    public Map<String, Object> getMaxDataDilution(double step_length, int gid_level,
                                                  Integer thread_num, Double startLat,
                                                  Double endLat, Double startLon, Double endLon,
                                                  String table_name) {
        return  null;
    }

    /**
     * 查询
     * 从存String的Hbase表中查询
     * 按最大值抽稀
     * @param step_length
     * @param gid_level
     * @param thread_num
     * @param startLat
     * @param endLat
     * @param startLon
     * @param endLon
     * @param table_name
     * @return
     */
    public Map<String, Object> getMaxDataOldDilution(double step_length, int gid_level,
                                                     Integer thread_num, Double startLat,
                                                     Double endLat, Double startLon, Double endLon,
                                                     String table_name) {
        Map<String, Object> map_return = new HashMap<>();
        //生成的csv文件列表的父目录
        String father_dir = CSVPath + DateUtil.formatDate(new Date(), "yyyyMMddHHmmss") + "/";
        String rowkey_prefix = "2018092903_0_";
        int split_interval = (int)(10.0/step_length) + 1;//计算每行，每列有多少格点数
        int split_interval_clean = (int)(10.0/(step_length * gid_level)) + 1;////计算抽希后，每行，每列有多少格点数
        //计算开始和结束纬度之间，需要在hbase中查询的所有rowkey的纬度拼接部分，升序
        List<String> rowkey_lat = RowKeyUtil.getRowKeyByLonOrLat(startLat,endLat);
        //计算开始和结束经度之间，需要在hbase中查询的所有rowkey的经度拼接部分，升序
        List<String> rowkey_lon = RowKeyUtil.getRowKeyByLonOrLat(startLon,endLon);
        Collections.reverse(rowkey_lon);//纬度范围变为降序
        //第一个纬度范围，开始的纬度坐标
        double start_lat = Double.parseDouble(rowkey_lat.get(0).split("_")[1]);
        //最后一个纬度范围，开始的纬度坐标
        double end_lat = Double.parseDouble(rowkey_lat.get(rowkey_lat.size() - 1).split("_")[1]);
        //第一个经度范围，开始的经度坐标，开始索引
        double start_lon = Double.parseDouble(rowkey_lon.get(0).split("_")[0]);
        //最后一个经度范围，开始的经度坐标，开始索引
        double end_lon = Double.parseDouble(rowkey_lon.get(rowkey_lon.size() - 1).split("_")[0]);

        Map<String, Long> map_all_select_time = new HashMap<>();//所有线程的查询时间
        long final_time_start = System.currentTimeMillis();//计算总时间-开始时间
        long final_time_total = 0l;//计算总时间-总时间
        ExecutorService service = Executors.newFixedThreadPool(thread_num);//线程池

        for(int i = 0; i < rowkey_lat.size(); i++) {
            final int i_run = i;
            //计算，在该纬度范围内的所有矩阵，纬度的开始索引和结束索引
            int start_lat_index = 0;
            int end_lat_index = split_interval_clean - 1;
            if(rowkey_lat.size() == 1) {
                end_lat_index = (int)CalculateUtil.divide(CalculateUtil.subtract(start_lat, startLat), step_length * gid_level, 2);
                start_lat_index = (int)CalculateUtil.divide(CalculateUtil.subtract(end_lat, endLat), step_length * gid_level, 2);
            }else {
                if(i_run == 0) {
                    start_lat_index = (int)CalculateUtil.divide(CalculateUtil.subtract(start_lat, endLat), step_length * gid_level, 2);
                }else if(i_run == rowkey_lat.size() - 1) {
                    end_lat_index = (int)CalculateUtil.divide(CalculateUtil.subtract(end_lat, startLat), step_length * gid_level, 2);
                }
            }
            final int final_end_lat_index = end_lat_index;
            final int final_start_lat_index = start_lat_index;
            //同一个纬度范围内，遍历所有经度范围的矩阵块
            for (int j = 0; j < rowkey_lon.size(); j++) {
                final int j_run = j;
                int start_lon_index = 0;
                int end_lon_index = split_interval_clean - 1;
                if(rowkey_lon.size() == 1) {
                    start_lon_index = (int)CalculateUtil.divide(CalculateUtil.subtract(startLon, start_lon), step_length * gid_level, 2);
                    end_lon_index = (int)CalculateUtil.divide(CalculateUtil.subtract(endLon, end_lon), step_length * gid_level, 2);
                }else {
                    if(j_run == 0) {
                        start_lon_index = (int)CalculateUtil.divide(CalculateUtil.subtract(start_lon, startLon), step_length * gid_level, 2);
                    }else if(j_run == rowkey_lon.size() - 1) {
                        end_lon_index = (int)CalculateUtil.divide(CalculateUtil.subtract(end_lon, endLon), step_length * gid_level, 2);
                    }
                }
                final int final_start_lon_index = start_lon_index;
                final int final_end_lon_index = end_lon_index;
                service.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            Connection conn = ConnectionFactory.createConnection(hbaseTemplate.getConfiguration());
                            TableName tableName = TableName.valueOf(table_name);
                            Table table = conn.getTable(tableName);
                            Get get = new Get(Bytes.toBytes(rowkey_prefix+rowkey_lat.get(i_run)+"_"+rowkey_lon.get(j_run) + "_" + step_length));
                            long select_time_start = System.currentTimeMillis();//查询开始时间
                            Result result = table.get(get);
                            long select_time_total = System.currentTimeMillis() - select_time_start;//单个矩阵
                            map_all_select_time.put(i_run+"_"+"j_run", select_time_total);
                            List<List<String>> row_arr2 = new ArrayList();//抽希前
                            List<List<String>> row_arr2_clean = new ArrayList();//抽希后

                            BufferedReader br = new BufferedReader(
                                    new InputStreamReader(new ByteArrayInputStream(result.value())));
                            String line;
                            int line_index = 0;//按行读取hbase中的矩阵，记录当前读取行数,第一行索引为0
                            while ((line = br.readLine()) != null) {
                                if(line_index > 0) {
                                    List<String> row_strs = Arrays.asList(line.split(","));
                                    row_strs = row_strs.subList(1, row_strs.size());
                                    row_arr2.add(row_strs);
                                }
                                line_index++;
                            }
                            for(int i = 0; i < row_arr2.size(); i++) {
                                if(i % gid_level == 0) {
                                    List<String> li = row_arr2.get(i);
                                    List<String> li_temp = new ArrayList<>();
                                    for(int j = 0; j < li.size(); j++) {
                                        if(j % gid_level == 0) {
                                            li_temp.add(li.get(j));
                                        }
                                    }
                                    row_arr2_clean.add(li_temp);
                                }
                            }
                            String path = father_dir+i_run+"_"+j_run+".csv";
//                                            CSVUtil.FloatArray2Csv(float_arr2, path);
                            table.close();
                            conn.close();

                        }catch(Exception ex) {
                            ex.printStackTrace();
                        }
                    }
                });
            }
        }
        service.shutdown();
        while(true){
            if(service.isTerminated()){
                System.out.println("一个子线程结束");
                break;
            }
        }
        final_time_total = System.currentTimeMillis() - final_time_start;
        map_return.put("total_time", final_time_total + "ms");
        for(Map.Entry<String, Long> m:map_all_select_time.entrySet()) {
            System.out.println("查询时间："+m.getValue());
        }
        return map_return;
    }
}
