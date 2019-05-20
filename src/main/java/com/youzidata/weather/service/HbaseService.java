package com.youzidata.weather.service;

import com.youzidata.weather.dao.HbaseDao;
import com.youzidata.weather.util.ListUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec._Label;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.data.hadoop.hbase.RowMapper;
import org.springframework.data.hadoop.hbase.TableCallback;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.RequestParam;

import com.youzidata.weather.util.CSVUtil;
import com.youzidata.weather.util.CreateZip;
import com.youzidata.weather.util.DoubleUtil;

import java.io.*;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Service
public class HbaseService {
	@Autowired
	private HbaseTemplate hbaseTemplate;

	@Autowired
	private HbaseDao hbaseDao;

	@Value("${CSVPath}")
	private String CSVPath;

	public Map<String, Object> getDateFromHbase(String startDate, Integer hour, String type, Integer decimal, List<String> layers, Double startLat,
			Double endLat, Double startLon, Double endLon) {
		CSVUtil util = new CSVUtil();
		DoubleUtil doubleUtil=new DoubleUtil();
		long start = new Date().getTime();
		long time = System.currentTimeMillis();
		int total = 0;
		List<String> listall = new ArrayList<>();
		Map<String, List<String>> map5 = new LinkedHashMap<>();
		Map<String, Long> sumChildLengthMap = new HashMap<>();
		ExecutorService executor = Executors.newFixedThreadPool(layers.size());
		for (String layer : layers) {
			List<String> listlayer = new ArrayList<>();
			executor.execute(new Runnable() {
				@Override
				public void run() {
//					System.out.println(layer);
					String result = hbaseTemplate.get("weather_" + startDate, startDate + hour + "_" + layer, "cf",
							type, new RowMapper<String>() {
								@Override
								public String mapRow(Result result, int rowNum) throws Exception {
									BufferedReader br = new BufferedReader(
											new InputStreamReader(new ByteArrayInputStream(result.value())));
									String line;
									int index = 0;
									int startIndex = 0;
									int endIndex = 0;
									String fileName = CSVPath + time + "/" + layer + ".csv";
									List<String> liresult = new ArrayList<>();
									while ((line = br.readLine()) != null) {
										if (!line.trim().equals("")) {
											String[] arr = line.split(",");
											if (index == 0) {

												if (startLat == null || startLat.equals("")) {
													startIndex = 1;
													endIndex = arr.length;
												} else {
													for (int i = 0; i < arr.length; i++) {
														if (i != 0) {
															if (Double.parseDouble(arr[i]) > startLon
																	&& startIndex < 1) {
																startIndex = i - 1;
															}
															if (Double.parseDouble(arr[i]) >= endLon && endIndex < 1) {
																endIndex = i + 1;
															}
														}
													}
												}
											}
											if (index != 0) {
												if ((startLat == null || startLat.equals("")) || (Double.parseDouble(arr[0]) >= startLat
														&& Double.parseDouble(arr[0]) <= endLat)) {
													String[] aaa = Arrays.copyOfRange(arr, startIndex, endIndex);
													String aaastr = StringUtils.join(aaa, ",");
													if (decimal != null) {
														aaastr = doubleUtil.doubleDecimal(decimal, aaa);
													}
													listlayer.addAll(Arrays.asList(aaastr.split(",")));
													String lineRes = arr[0] + "," + aaastr;
													liresult.add(lineRes);

												}
											} else {
												String[] aaa = Arrays.copyOfRange(arr, startIndex, endIndex);
												String lineRes = "," + StringUtils.join(aaa, ",");
												liresult.add(lineRes);
											}
										}
										index++;
										// }
									}
									File file = util.String2Csv(liresult, fileName);
									sumChildLengthMap.put(System.currentTimeMillis() + "", file.length());
									return "";
								}
							});

				}
			});
			map5.put(layer, listlayer);
		}

		executor.shutdown();
		while (true) {
			if (executor.isTerminated()) {
				System.out.println("所有的子线程都结束了！");
				break;
			}
		}
		long zipTime = System.currentTimeMillis();
		String newFilePath = CSVPath + time + ".zip ";
		String cmmand = "zip -r " + newFilePath  + time + "/";
		String[] cmds = new String[3];
		cmds[0] = "/bin/bash";
		cmds[1] = "-c";
		cmds[2] = "cd "+CSVPath +" && "+cmmand;
		try {
			Process a = Runtime.getRuntime().exec(cmds);
			 a.waitFor();
		} catch (Exception e) {
			e.printStackTrace();
		}
//		CreateZip.createZip(CSVPath + time , newFilePath);
		long end = new Date().getTime();

		List<List<Double>> li = new ArrayList<>();
		for (Entry<String, List<String>> en : map5.entrySet()) {
			List<String> qqq=en.getValue();
			Collections.sort(qqq);
			listall.addAll(en.getValue());
			List<Double> Listlim = new ArrayList<>();
			Listlim.add(Double.parseDouble(en.getValue().get(en.getValue().size()-1)));
			Listlim.add(Double.parseDouble(en.getValue().get(0)));
			li.add(Listlim);
		}
		Set<Entry<String, Long>> entrySet3 = sumChildLengthMap.entrySet();
		long lenth = 0;
		for (Entry<String, Long> map : entrySet3) {
			lenth += map.getValue();
		}
		File newZipFile = new File(newFilePath);
		String resultPath = "/loadFile?fileName="+time;
		Map<String, Object> resultMap = new LinkedHashMap<>();
		resultMap.put("total", listall.size());
		resultMap.put("limitType", li);
		resultMap.put("fileNum", 1);
		resultMap.put("filePath", resultPath);
		DecimalFormat dec = new DecimalFormat("0.0000");
		resultMap.put("fileFormat", "ZIP");
		resultMap.put("fileName", time + ".zip");
		resultMap.put("fileSize", dec.format(newZipFile.length() / (1024 + 0.0) / (1024 + 0.0)) + " MB");
		resultMap.put("decompressionFileSize", dec.format(lenth / (1024 + 0.0) / (1024 + 0.0)) + " MB");
		long zipEndTime = System.currentTimeMillis();
		resultMap.put("sumTime", end - start);
		resultMap.put("selectTime", zipTime - start);
		resultMap.put("zipTime", end - zipTime);
		return resultMap;
	}

	//测试用，查询Hbase数据,返回查到的二维数组
	public List<List<String>> getDateFromHbaseTest(String startDate, Integer hour, String type, String layer, Double startLat, Double endLat, Double startLon, Double endLon){
		List<List<String>> result=new ArrayList<>();
		result = hbaseTemplate.get("weather_" + startDate, startDate + hour + "_" + layer,"cf", type,
				new RowMapper<List<List<String>>>() {
					@Override
					public List<List<String>> mapRow(Result result, int rowNum) throws Exception {
						List<List<String>> temp=new ArrayList<>();
						BufferedReader br = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(result.value())));
						String line;
						while ( (line = br.readLine()) != null ) {
							if(!line.trim().equals("")){
								temp.add(Arrays.asList(line.split(",")));
							}
						}
						return temp;
					}
				});
		return result;
	}

	//测试用，查询Hbase数据
	public Map<String,List<String>> getTestDateFromHbase(String startDate, Integer hour, String type, List<String> layers, Double startLat, Double  endLat, Double startLon, Double endLon){
		ExecutorService executor = Executors.newFixedThreadPool(layers.size());
		Map<String,List<String>> map=new HashMap<>();
		for(String layer : layers){
			executor.execute(new Runnable() {
				@Override
				public void run() {
//                    System.out.println(layer);
					List<String> result = hbaseTemplate.get("weather_" + startDate, startDate + hour + "_" + layer,"cf", type,
							new RowMapper<List<String>>() {
								@Override
								public List<String> mapRow(Result result, int rowNum) throws Exception {
									List<String> temp=new ArrayList<>();
									BufferedReader br = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(result.value())));
									String line;
									while ( (line = br.readLine()) != null ) {
										if(!line.trim().equals("")){
											temp.add(line);
										}
									}
									return temp;
								}
							});
					map.put(layer,result);
				}
			});
		}

		executor.shutdown();
		while(true){
			if(executor.isTerminated()){
				System.out.println("所有的子线程都结束了！");
				break;
			}
		}

		return map;
	}

	//Hbase更新-测试
	public Object updateDataToHbaseTest(String startDate, Integer hour, String type, Integer layer, String interval) throws IOException {
		Map<String,String> map=testData(interval, layer);
		Map<String,Object> resultMap = new HashMap<>();
		ExecutorService executor = Executors.newFixedThreadPool(3);

		Map<String,List<List<String>>> map_data=new HashMap<>();
		for (Entry<String, String> en : map.entrySet()) {
			List<List<String>> list=new ArrayList<>();
			 String[] arr=en.getValue().split("\\\n");
			 for (String string : arr) {
				 List<String> list1=new ArrayList<>();
				 String[] arr1=string.split(",");
				 list1.addAll(Arrays.asList(arr1));
				 list.add(list1);
			}
			 map_data.put(en.getKey(), list);
		}
		long startTime=new Date().getTime();
		System.out.println("开始更新");
		for(Map.Entry<String,List<List<String>>> m:map_data.entrySet()){//按高度层遍历
			executor.execute(new Runnable() {//向线程池中添加线程
				@Override
				public void run() {//一定要捕获子线程异常，不然它出轨了都不知道
					try {
						List<List<String>> list_request=m.getValue();//模拟前端传过来的矩阵
						int row_count=list_request.size();//行数
						int column_count=list_request.get(0).size();//列数
						Double lon_1;
						Double lat_1;
						Double lon_2;
						Double lat_2;
						lon_1=Double.valueOf(list_request.get(0).get(1));
						lat_1=Double.valueOf(list_request.get(1).get(0));
						lon_2=Double.valueOf(list_request.get(0).get(column_count-1));
						lat_2=Double.valueOf(list_request.get(row_count-1).get(0));

						List<List<String>> list_hbase=getDateFromHbaseTest(startDate,hour,type, m.getKey(),lat_1,lat_2,lon_1,lon_2);
						int row_count_hbase=list_request.size();//行数
						int column_count_hbase=list_request.get(0).size();//列数
						List<Integer> lon_index=new ArrayList<>();
						List<Integer> lat_index=new ArrayList<>();
						String firstPoint=list_request.get(1).get(1);
						String endPoint=list_request.get(list_request.size()-1).get(list_request.size()-1);
						for(int i=0;i<list_hbase.size();i++){
							if(lat_1.toString().equals(list_hbase.get(i).get(0))||lat_2.toString().equals(list_hbase.get(i).get(0))){
								lat_index.add(i);
							}
						}
						for(int j = 0; j< list_hbase.get(0).size(); j++){
							if(lon_1.toString().equals(list_hbase.get(0).get(j))||lon_2.toString().equals(list_hbase.get(0).get(j))){
								lon_index.add(j);
							}
						}
						int aa=1;int bb=1;
						for(int i= lat_index.get(0);i<= lat_index.get(1);i++) {
							for (int j=lon_index.get(0);j<= lon_index.get(1); j++) {
								list_hbase.get(i).set(j,list_request.get(aa).get(bb));
								bb++;
							}
							bb=1;
							aa++;
						}
						StringBuffer sBuffer = new StringBuffer();
						for(List<String> li:list_hbase){
							sBuffer.append(StringUtils.join(li,","));
							sBuffer.append("\n");
						}

						//更新Hbase
						hbaseDao.updateTable("weather_"+startDate,startDate+hour+"_"+m.getKey(),"cf",type,sBuffer.toString());



					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			});

		}
		executor.shutdown();
		//等待子线程全都执行完，再执行后面的逻辑
		try {
			executor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		long endTime=new Date().getTime();
		System.out.println("更新时间："+(endTime-startTime));
//		return endTime-startTime;
		resultMap.put("code", "200");
		resultMap.put("updateTime", endTime-startTime);
		return resultMap;
	}
	
	public Object updateDataToHbase(String startDate, Integer hour, String type, Map<String,String> map) throws IOException {
		Map<String,Object> resultMap = new HashMap<>();
		ExecutorService executor = Executors.newFixedThreadPool(3);

		Map<String,List<List<String>>> map_data=new HashMap<>();
		for (Entry<String, String> en : map.entrySet()) {
			List<List<String>> list=new ArrayList<>();
			 String[] arr=en.getValue().split("\\\n");
			 for (String string : arr) {
				 List<String> list1=new ArrayList<>();
				 String[] arr1=string.split(",");
				 list1.addAll(Arrays.asList(arr1));
				 list.add(list1);
			}
			 map_data.put(en.getKey(), list);
		}
		long startTime=new Date().getTime();
		System.out.println("开始更新");
		for(Map.Entry<String,List<List<String>>> m:map_data.entrySet()){//按高度层遍历
			executor.execute(new Runnable() {//向线程池中添加线程
				@Override
				public void run() {//一定要捕获子线程异常，不然它出轨了都不知道
					try {
						List<List<String>> list_request=m.getValue();//模拟前端传过来的矩阵
						int row_count=list_request.size();//行数
						int column_count=list_request.get(0).size();//列数
						Double lon_1;
						Double lat_1;
						Double lon_2;
						Double lat_2;
						lon_1=Double.valueOf(list_request.get(0).get(1));
						lat_1=Double.valueOf(list_request.get(1).get(0));
						lon_2=Double.valueOf(list_request.get(0).get(column_count-1));
						lat_2=Double.valueOf(list_request.get(row_count-1).get(0));

						List<List<String>> list_hbase=getDateFromHbaseTest(startDate,hour,type, m.getKey(),lat_1,lat_2,lon_1,lon_2);
						int row_count_hbase=list_request.size();//行数
						int column_count_hbase=list_request.get(0).size();//列数
						List<Integer> lon_index=new ArrayList<>();
						List<Integer> lat_index=new ArrayList<>();
						String firstPoint=list_request.get(1).get(1);
						String endPoint=list_request.get(list_request.size()-1).get(list_request.size()-1);
						for(int i=0;i<list_hbase.size();i++){
							if(lat_1.toString().equals(list_hbase.get(i).get(0))||lat_2.toString().equals(list_hbase.get(i).get(0))){
								lat_index.add(i);
							}
						}
						for(int j = 0; j< list_hbase.get(0).size(); j++){
							if(lon_1.toString().equals(list_hbase.get(0).get(j))||lon_2.toString().equals(list_hbase.get(0).get(j))){
								lon_index.add(j);
							}
						}
						int aa=1;int bb=1;
						for(int i= lat_index.get(0);i<= lat_index.get(1);i++) {
							for (int j=lon_index.get(0);j<= lon_index.get(1); j++) {
								list_hbase.get(i).set(j,list_request.get(aa).get(bb));
								bb++;
							}
							bb=1;
							aa++;
						}
						StringBuffer sBuffer = new StringBuffer();
						for(List<String> li:list_hbase){
							sBuffer.append(StringUtils.join(li,","));
							sBuffer.append("\n");
						}

						//更新Hbase
						hbaseDao.updateTable("weather_"+startDate,startDate+hour+"_"+m.getKey(),"cf",type,sBuffer.toString());



					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			});

		}
		executor.shutdown();
		//等待子线程全都执行完，再执行后面的逻辑
		try {
			executor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		long endTime=new Date().getTime();
		System.out.println("更新时间："+(endTime-startTime));
		resultMap.put("code", "200");
		resultMap.put("updateTime", endTime-startTime);
		return resultMap;
	}

	public static Map<String,String> testData(String layer,int interval){
		Map<String,String> map=new HashMap<>();
		if(layer.equals("0.125")){
			String lon=",";
			for (double i = 60; i <=150; i=(i+0.125)) {
				if(i==150){
					lon+=i+"\n";
				}else{
					lon+=i+",";
				}
			}
			String s="";
			for (int j = 0; j <=720; j++) {
				double re=Math.random();
				if(j==720){
					s+=re;
				}else{
					s+=re+",";
				}
			}
			String line="";
			for (double i = -10; i <= 60; i=(i+0.125)) {
				String i1=(i+"");
				line+=i1+","+s+"\n";
			}
//			System.out.println(lon+line);
			map.put(String.valueOf(0), lon+line);
		}else{
			String lon=",";
			for (double i = 60; i <=150; i=(i+0.25)) {
				if(i==150){
					lon+=i+"\n";
				}else{
					lon+=i+",";
				}
			}
			for (int z = 1; z <=interval; z++) {
				String s="";
				for (int j = 0; j <=360; j++) {
					double re=Math.random();
					if(j==360){
						s+=re;
					}else{
						s+=re+",";
					}
				}
				String line="";
				for (double i = -10; i <= 60; i=(i+0.25)) {
					String i1=(i+"");
					line+=i1+","+s+"\n";
				}
//				System.out.println(lon+line);
				map.put(String.valueOf(z), lon+line);
			}
		}
		return map;
	}

}
