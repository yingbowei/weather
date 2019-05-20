package com.youzidata.weather.task;

import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;

import com.youzidata.weather.dao.HbaseDao;
import com.youzidata.weather.hbase.DataSourceConfig;
import com.youzidata.weather.util.DateUtil;
import com.youzidata.weather.util.FileUtil;
import com.youzidata.weather.util.StringUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.jcodings.util.Hash;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.hbase.HbaseSystemException;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.data.hadoop.hbase.TableCallback;
import org.springframework.stereotype.Component;

import com.alibaba.fastjson.JSONObject;
import com.amazonaws.services.kms.model.transform.RetireGrantResultJsonUnmarshaller;

import org.springframework.stereotype.Service;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.Dimension;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

//@Component
@Service
public class LoadToHbaseTask {

    @Autowired
    private HbaseTemplate hbaseTemplate;

    @Autowired
    private HbaseDao hbaseDao;


	private static byte[] family = Bytes.toBytes("cf");

    private byte[][] getSplitKeys(String date) {
        String[] keys = new String[] { date+"120_", date+"147_", date+"171_", date+"195_", date+"219_",date+"27_",date+"51",date+"75"};
//        String[] keys = new String[] { date+"0_", date+"30_", date+"60_", date+"90_", date+"120_",date+"150_",date+"180_",date+"210_",date+"240_"};
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

    public Map<String, Object> test(String  dirPath, String startDate, String endDate) throws ParseException {
        SimpleDateFormat sdf1 = new SimpleDateFormat("yyyyMMddHHmmss");
//        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
//        Date s_date = sdf.parse(startDate);
//        Date e_date = sdf.parse(endDate);
        System.out.println(dirPath);
		long startTime = System.currentTimeMillis();
		System.out.println("-----"+"开始执行："+startTime);
//        List<String> list_date = DateUtil.getDayRangeStrList(startDate, endDate, "yyyyMMdd", "yyyyMMdd");
        List<String> fileList = new ArrayList<>();//筛选出来需要入库的文件路径
        List<File> filelist = FileUtil.getNewestFileAndNoUserId(dirPath, startDate, endDate);//

//        Map<String, File> map_time_path = new HashMap();//查询每个文件的最近修改时间，作为key
//        for(File file:filelist) {
////            String date_str = StringUtil.getDateStrFromNc(file.getName(), 8);
////            Date this_date = sdf.parse(date_str);
////            if(this_date.compareTo(s_date) >= 0 && this_date.compareTo(e_date) <= 0) {
//            //获取服务器上面文件的修改时间
////            String time = FileUtil.getModifyTimeFromCentos(file.getPath());
////            map_time_path.put(time, file);
//            //本地测试用
//            String time = sdf1.format(new Date());
//            map_time_path.put(time, file);
//            try {
//                Thread.sleep(500);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
////            }
//        }
//        Map<String, String> map_path_path = new LinkedHashMap<>();//文件名作为key，文件全路径作为value
//        for(Map.Entry<String, File> m:map_time_path.entrySet()) {
//            String path = m.getValue().getName();
////            String [] strArr1 = path.split("_");
////            String [] strArr2 = new String [strArr1.length - 1];
////            System.arraycopy(strArr1, 0, strArr2, 0, strArr1.length - 1);
////            String key = StringUtils.join(strArr2, "_");
//            map_path_path.put(path, m.getValue().getPath());
//        }
//        for(Map.Entry<String, String> m:map_path_path.entrySet()) {
//            System.out.println(m.getValue());
//            fileList.add(m.getValue());
//        }
        for(File file:filelist) {
            fileList.add(file.getPath());
        }
        long readFile = System.currentTimeMillis();//读取所有nc文件路径的开始时间
        System.out.println("-----"+"读取所有nc文件路径时间:"+(readFile - startTime));
//        File file_temp = new File(fileList.get(0));
//        System.out.println("file.getName():"+ file_temp.getName());
        Set<String> table_name_date = new HashSet<>();
//        for(Map.Entry<String, String> m:map_path_path.entrySet()) {
//            File file = new File(m.getValue());
//            String date = StringUtil.getDateStrFromNc(file.getName(), 10);
//            table_name_date.add(date);
//        }
        //获取文件名中的时间，根据时间获取表名，下面会进行表名是否存在的判断，不存在会执行建表语句
        for(File file:filelist) {
            String date = StringUtil.getDateStrFromNc(file.getName(), 10);
            table_name_date.add(date);
        }
        System.out.println("-----"+"文件数量:"+fileList.size());
        //判断表名是否存在，不存在则创建该表
        for(String str:table_name_date) {
            byte[][] splitKeys = getSplitKeys(str);
            //判断该表是否存在，不存在就建表
            hbaseDao.createTable("weather_"+str, splitKeys);
        }
        Map<String, Object> resultMap = loadList(fileList);

        long endTime = System.currentTimeMillis();
		System.out.println("-----"+"程序结束:"+endTime);
		System.out.println("-----"+"总共用时："+(endTime-startTime));
		resultMap.put("sumTime", endTime-startTime);
		return  resultMap;
	}

    public  Map<String,Object> loadList(List<String> fileList){
        ExecutorService executor = Executors.newFixedThreadPool(2);
        Map<String, Long> writeMap = new HashMap<>();
        Map<String, Long> nanMap = new HashMap<>();
        Map<String, Long> readMap = new HashMap<>();
        List<String> errorPath = new ArrayList<>();
////		System.out.println(fileList.toString());
        try {
            for(String path :fileList ) {
                executor.execute(new Runnable() {//向线程池中添加线程
                    @Override
                    public void run() {
                        try {
                            Boolean result = load(path,nanMap,writeMap,readMap);
                            if(!result) {
                                System.out.println("-----"+path+"批量添加失败");
                                errorPath.add(path);
                            }
//                            Thread.sleep(1000);
                        } catch (Exception e) {
                            errorPath.add(path);
                            e.printStackTrace();
                        }
                    }
                });
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        executor.shutdown();
        while(true){
//        	try {
//				Thread.sleep(5000);
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}
            if(executor.isTerminated()){
                System.out.println("-----所有的子线程都结束了！");
                break;
            }
        }

        if(errorPath != null && errorPath.size()>0) {
            System.out.println("-----NC文件导入失败文件数"+errorPath.size());
//            try {
////				Thread.sleep(10000);
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}
            for(String str : errorPath){
                load(str,nanMap,writeMap,readMap);
            }
        }
        Set<Entry<String, Long>> entrySet = nanMap.entrySet();
        long nnaTime = 0;
        for(Entry<String, Long> entry : entrySet) {
        	nnaTime += entry.getValue();
        }
        System.out.println("-----"+"解析共消耗:"+nnaTime);
        long writeTime = 0;
        Set<Entry<String, Long>> entrySet2 = writeMap.entrySet();
        for(Entry<String, Long> entry : entrySet2) {
        	writeTime += entry.getValue();
        }
        System.out.println("-----写文件共消耗:" +writeTime);
        long readTime = 0;
        Set<Entry<String, Long>> entrySet3 = readMap.entrySet();
        for(Entry<String, Long> entry : entrySet3) {
            readTime += entry.getValue();
        }
        System.out.println("-----读文件共消耗:" +readTime);
        Map<String, Object> resultMap = new HashMap<String, Object>();
        resultMap.put("readTime", readTime);
        resultMap.put("analysisTime", nnaTime);
        resultMap.put("writeTime", writeTime);
        return resultMap;
    }

//	@Profile("prod_task")
//    @Scheduled(cron = "47 12 * * * *")
	public  boolean load(String path,Map<String, Long> anaMap,Map<String, Long> writeMap,Map<String, Long> readMap){
        long start = new Date().getTime();
        boolean res=false;
        try {
        	long startTime= System.currentTimeMillis();
			NetcdfFile openNC = NetcdfFile.open(path);
			long endTime = System.currentTimeMillis();
			System.out.println("-----打开文件用时:"+(endTime - startTime));
//			String date = path.substring(path.length()-29, path.length()-19);
//			int hour = Integer.parseInt(path.substring(path.length()-10, path.length()-7));

            File file_temp = new File(path);
            String fileName = file_temp.getName();
//            String date = StringUtil.getDateStrFromNc(file_temp.getName(), 10);
//            int hour = StringUtil.getPreHourFromNc(file_temp.getName());
//            String userId = StringUtil.getUserIdFromNc(file_temp.getName());

			res = parseFile(fileName,openNC,anaMap,writeMap,readMap);
			long end = System.currentTimeMillis();
			System.out.println("-----解析加保存用时："+(end - endTime));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return res;
		}

		return res;
	}

//	public static void main(String[] args) {
//		String filePath = "C:\\maitu\\1233\\MODP_TJQX_ECNC_ADCN_2018020612-003.nc";
//		String date = filePath.substring(filePath.length() - 17, filePath.length()-7);
//		int hour = Integer.parseInt(filePath.substring(filePath.length() -6, filePath.length()-3));
//		try {
//			NetcdfFile open = NetcdfFile.open(filePath);
//			boolean parseFile = parseFile(date,hour,open);
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//	}
	/**
	 *
	 * @Title: parseFile
	 * @author: Mr man
	 * @Description: 具体解析文件的方法
	 * @param: @param date   从文件名截取的时间+0000  组成   yyyyMMddHHmmss
	 * @param: @param hour   小时范围
	 * @param: @param newTime  具体存储的时间
	 * @param: @param indexEnd  动态索引尾部
	 * @param: @param openNC   具体NC文件
	 * @param: @return
	 * @return: boolean
	 * @throws
	 */
	public  boolean parseFile(String fileName, NetcdfFile openNC,Map<String, Long> nanMap,Map<String, Long> wirteMap,Map<String, Long> readMap){
        String date = StringUtil.getDateStrFromNc(fileName, 10);
        String userId = StringUtil.getUserIdFromNc(fileName);
        int timeRange = StringUtil.getPreHourFromNc(fileName);;
        boolean res = true;
        long analysisStartTime = System.currentTimeMillis();
        List<Put> puts = new ArrayList<>();
        try {
            List<Variable> variables = openNC.getVariables();
            Map<String, Object> demensionsMap = new HashMap<>();
            List<Dimension> dimensions = openNC.getDimensions();
            for (Dimension dimension : dimensions) {
                if (dimension.getShortName().contains("lon")) {
                    Variable latLonVariable = openNC.findVariable(dimension.getShortName());
                    float[] latLonArray = parseFileOne(latLonVariable.getDimensions(), latLonVariable);
                    StringBuilder stringBuilder = new StringBuilder();
//                    stringBuilder.append(",");
//                    stringBuilder.append(Arrays.toString(latLonArray).replace("[","").replace("]",""));
                    for (int i = 0; i < latLonArray.length; i++) {
                        stringBuilder.append(",");
                        stringBuilder.append(latLonArray[i]);
                    }
                    stringBuilder.append("\n");
                    demensionsMap.put(dimension.getShortName() + "_title", stringBuilder);
                }else if(dimension.getShortName().contains("lat")){
                    Variable latLonVariable = openNC.findVariable(dimension.getShortName());
                    float[] latLonArray = parseFileOne(latLonVariable.getDimensions(), latLonVariable);
                    demensionsMap.put(dimension.getShortName() + "_title", latLonArray);
                }
            }

            int latThreelength = 0;
            int lonThreelength = 0;
            int lvThreeLength = 0;
            List<Integer> lvThreeValue = new ArrayList<>();
            int latTwoLength = 0;
            int lonTwoLength = 0;
            float[] latTwo = null;
            float[] latThree = null;
            List<Map<String, Object>> twoArrayList = new ArrayList<>();
            List<Map<String, Object>> threeArrayList = new ArrayList<>();
            List<Map<String, Object>> twoThreeArrayList = new ArrayList<>();
//            Map<String, Variable> variableMap = new HashMap<>();
            Map<String, Object> variableArrayMap = new HashMap<>();

            Map<String,Future<List<String>>> threeMap = new HashMap<>();
            Map<String,Future<String>> twoMap = new HashMap<>();

            ExecutorService service = Executors.newFixedThreadPool(36);

            for (Variable variable : variables) {
                if (variable.getDimensions().size() == 3) {
                    lvThreeLength = variable.getDimension(0).getLength();
                    Dimension lvThreeDimension = variable.getDimension(0);
                    String fullName = lvThreeDimension.getFullName();
                    int [] i_arr = (int[])lvThreeDimension.getGroup().findVariable(fullName).read().copyTo1DJavaArray();
                    for(int i:i_arr) {
                        lvThreeValue.add(i);
                    }
                    Dimension londimension = variable.getDimension(variable.getDimensions().size() - 1);
                    String lonstr = demensionsMap.get(londimension.getShortName() + "_title").toString();
                    Dimension latdimension = variable.getDimension(variable.getDimensions().size() - 2);
                    float[] lat = (float[]) demensionsMap.get(latdimension.getShortName() + "_title");

                    int[] org = new int[3];
                    int[] msg = new int[3];

                    for(int i=0;i<variable.getDimensions().size();i++) {
                        org[i] = 0;
                        msg[i] = variable.getDimensions().get(i).getLength();
                    }
                    float[][][] resultData = (float[][][]) variable.read(org, msg).copyToNDJavaArray();

                    ParseThreeThread thread = new ParseThreeThread(resultData,lonstr,lat);
                    threeMap.put(variable.getShortName(),service.submit(thread));

                }else if(variable.getDimensions().size() == 2){
                    Dimension londimension = variable.getDimension(variable.getDimensions().size() - 1);
                    String lonstr = demensionsMap.get(londimension.getShortName() + "_title").toString();
                    Dimension latdimension = variable.getDimension(variable.getDimensions().size() - 2);
                    float[] lat = (float[]) demensionsMap.get(latdimension.getShortName() + "_title");

                    int[] org = new int[variable.getDimensions().size()];
                    int[] msg = new int[variable.getDimensions().size()];
                    for(int i=0;i<variable.getDimensions().size();i++) {
                        org[i] = 0;
                        msg[i] = variable.getDimensions().get(i).getLength();
                    }
                    float[][] resultData = (float[][])variable.read(org, msg).copyToNDJavaArray();

                    ParseTwoThread thread = new ParseTwoThread(resultData,lonstr,lat);
                    twoMap.put(variable.getFullName(),service.submit(thread));
                }
            }
            readMap.put(date+"read", System.currentTimeMillis() - analysisStartTime);
            System.out.println("==========数据读取时间======            "+(System.currentTimeMillis() - analysisStartTime));
            analysisStartTime = System.currentTimeMillis();

            service.shutdown();
            while(true){
                if(service.isTerminated()){
                    break;
                }
            }
            System.out.println("==========解析时间======                "+(System.currentTimeMillis() - analysisStartTime));
            nanMap.put(date+"ana", System.currentTimeMillis() - analysisStartTime);

            long writeStartTime =System.currentTimeMillis();


//            ExecutorService inservice = Executors.newFixedThreadPool(10);
//
//            HConnection connection = HConnectionManager.createConnection(hbaseTemplate.getConfiguration());
//
//            HTableInterface table = connection.getTable("weather_"+date, inservice);
//            table.setAutoFlush(false);

            try{
                for(int i = 0; i < lvThreeValue.size(); i++) {
                    String rowkey = date + "_" + timeRange + "_" + lvThreeValue.get(i) + "_" + userId;
                    Put put = new Put(Bytes.toBytes(rowkey));
                    for(Entry<String,Future<List<String>>> entry : threeMap.entrySet()){
                        put.addColumn(family, Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue().get().get(i).toString()));
                    }
                    puts.add(put);
                }
//                for(int i=0;i<lvThreeLength;i++){
//                    String rowkey = date + hour + "_" + (i+1);
//                    Put put = new Put(Bytes.toBytes(rowkey));
//                    for(Entry<String,Future<List<String>>> entry : threeMap.entrySet()){
//                        put.addColumn(family, Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue().get().get(i).toString()));
//                    }
//                    puts.add(put);
//                }
                //如果是二维属性，没有时效参数，那么时效统一默认为999
                String rowkey = date + "_" + timeRange + "_" + 999 + "_" + userId;
                for(Entry<String,Future<String>> entry : twoMap.entrySet()){
                    Put put = new Put(Bytes.toBytes(rowkey));
                    put.addColumn(family, Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue().get().toString()));
                    puts.add(put);
                }
                String tablename = "weather_"+date;
                hbaseDao.multithreadingInsert(tablename, puts, 10);
            }catch(Exception e){
                e.printStackTrace();
            }finally {
            }

            long writeEndTime = System.currentTimeMillis();
            System.out.println("===========================================");
            System.out.println("==================单个文件写数据时间共消耗========            "+(writeEndTime - writeStartTime));
            wirteMap.put(date+"write", writeEndTime - writeStartTime);

        } catch (Exception ex){
            ex.printStackTrace();
            res = false;
        }finally {
            if(openNC!=null){
                try {
                    openNC.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

		return res;
	}


	/*
	 * 解析三维
	 */
	public static float[][][] parseFileThree(List<Dimension> dimensions,Variable n) throws Exception {
        int[] org = new int[3];
        int[] msg = new int[3];
        for(int i=0;i<dimensions.size();i++) {
            org[i] = 0;
            msg[i] = dimensions.get(i).getLength();
        }
        float[][][] resultData = new float[msg[0]][msg[1]][msg[2]];
        int length = msg[0];
        msg[0] = 1;
        //分层读取文件  具体分的层数  目前是取决于第一维度的大小
        for(int i=0;i<length;i++) {
            org[0] = i;
            float[][][] data = (float[][][])n.read(org, msg).copyToNDJavaArray();
            for(int m=0;m<data.length;m++){
                for(int l=0;l<data[m].length;l++){
                    for(int p =0;p<data[m][l].length;p++) {
                        resultData[i][l][p] = data[m][l][p];
                    }
                }
            }
        }
        return resultData;
    }


	/*
	 * 解析二维
	 */
	public float[][] parseFileTwo(List<Dimension> dimensions,Variable n) throws Exception {
		int[] org = new int[dimensions.size()];
		int[] msg = new int[dimensions.size()];
		for(int i=0;i<dimensions.size();i++) {
			org[i] = 0;
			msg[i] = dimensions.get(i).getLength();
		}
		float[][] resultData = new float[msg[0]][msg[1]];
		int length = msg[0];
		msg[0] = 1;
		//分层读取文件  具体分的层数  目前是取决于第一维度的大小
		for(int i=0;i<length;i++) {
			org[0] = i;
			float[][] data = (float[][])n.read(org, msg).copyToNDJavaArray();
			for(int m=0;m<data.length;m++){
				for(int l=0;l<data[m].length;l++){
						resultData[i][l] = data[m][l];
				}
			}
		}
		return resultData;
	}

    public String parseFileTwo1(String lon, float[] lat,List<Dimension> dimensions,Variable n){

        StringBuilder keystr = new StringBuilder();
        keystr.append(lon);

        try {

        int[] org = new int[dimensions.size()];
        int[] msg = new int[dimensions.size()];
        for(int i=0;i<dimensions.size();i++) {
            org[i] = 0;
            msg[i] = dimensions.get(i).getLength();
        }
        float[][] resultData = new float[msg[0]][msg[1]];
        int length = msg[0];
        msg[0] = 1;
        //分层读取文件  具体分的层数  目前是取决于第一维度的大小
        for(int i=0;i<length;i++) {
            org[0] = i;
            float[][] data = (float[][])n.read(org, msg).copyToNDJavaArray();
            keystr.append(lat[i]);
            for(int l=0;l<data[0].length;l++){
                keystr.append(",");
                keystr.append(data[0][l]);
            }
            keystr.append("\n");
        }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return keystr.toString();
    }

	/*
	 * 一维数组
	 */
	public static float[] parseFileOne(List<Dimension> dimensions,Variable n) throws Exception {
	    float[] data = (float[])n.read().copyToNDJavaArray();
		return data;
	}
}
