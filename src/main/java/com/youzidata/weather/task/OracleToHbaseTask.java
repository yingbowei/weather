//package com.youzidata.weather.task;
//
//import java.io.File;
//import java.io.IOException;
//import java.text.DecimalFormat;
//import java.util.*;
//import java.util.Map.Entry;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
//import java.util.concurrent.TimeUnit;
//
//import com.youzidata.weather.entity.ViewJsCdStationEntity;
//import com.youzidata.weather.hbase.DataSourceConfig;
//import com.youzidata.weather.service.WeatherService;
//
//import org.apache.hadoop.hbase.HColumnDescriptor;
//import org.apache.hadoop.hbase.HTableDescriptor;
//import org.apache.hadoop.hbase.TableName;
//import org.apache.hadoop.hbase.client.Admin;
//import org.apache.hadoop.hbase.client.Connection;
//import org.apache.hadoop.hbase.client.ConnectionFactory;
//import org.apache.hadoop.hbase.client.HTableInterface;
//import org.apache.hadoop.hbase.client.Put;
//import org.apache.hadoop.hbase.util.Bytes;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.data.hadoop.hbase.HbaseTemplate;
//import org.springframework.data.hadoop.hbase.TableCallback;
//import org.springframework.stereotype.Component;
//
//import com.alibaba.fastjson.JSONObject;
//
//import org.springframework.stereotype.Service;
//import ucar.nc2.Dimension;
//import ucar.nc2.NetcdfFile;
//import ucar.nc2.Variable;
//
////@Component
//@Service
//public class OracleToHbaseTask {
//
//	@Autowired
//	private HbaseTemplate hbaseTemplate;
//
//	@Autowired
//	WeatherService weatherService;
//
//	private static byte[] family = Bytes.toBytes("cf");
//
//	public void test() {
//		// long startTime = System.currentTimeMillis();
//		// System.out.println(startTime);
//		// Admin admin = null;
//		// Connection conn = null;
//		// try {
//		// // admin = DataSourceConfig.connection.getAdmin();
//		// conn =
//		// ConnectionFactory.createConnection(hbaseTemplate.getConfiguration());
//		// admin = conn.getAdmin();
//		// if (!admin.isTableAvailable(TableName.valueOf("VIEW_JS_CD_STATION")))
//		// {
//		// HTableDescriptor hbaseTable = new
//		// HTableDescriptor(TableName.valueOf("VIEW_JS_CD_STATION"));
//		// hbaseTable.addFamily(new HColumnDescriptor("cf"));
//		// admin.createTable(hbaseTable);
//		// }
//		// } catch (IOException e) {
//		// e.printStackTrace();
//		// } finally {
//		// try {
//		// admin.close();
//		// conn.close();
//		// } catch (IOException e) {
//		// e.printStackTrace();
//		// }
//		// }
//		List<ViewJsCdStationEntity> list = weatherService.selectViewJsCdStation();
//		parseFile(list);
//
//		// long endTime = System.currentTimeMillis();
//		// System.out.println(endTime);
//		// System.out.println("=========================");
//		// System.out.println(endTime - startTime);
//		// System.out.println("共计" + (endTime - startTime));
//	}
//
//	@SuppressWarnings("deprecation")
//	public boolean parseFile(List<ViewJsCdStationEntity> list) {
//		List<Put> puts = new ArrayList<>();
//		for (ViewJsCdStationEntity entity : list) {
//			String rowkey = entity.getV01000();
//			Put put = new Put(Bytes.toBytes(rowkey));
//			try {
//				put.addColumn(family, Bytes.toBytes("V05001"), Bytes.toBytes(entity.getV05001().toString()));
//			} catch (Exception e) {
//			}
//			try {
//				put.addColumn(family, Bytes.toBytes("V06001"), Bytes.toBytes(entity.getV06001().toString()));
//			} catch (Exception e) {
//			}
//			try {
//				put.addColumn(family, Bytes.toBytes("V07001"), Bytes.toBytes(entity.getV07001().toString()));
//			} catch (Exception e) {
//			}
//			try {
//				put.addColumn(family, Bytes.toBytes("C_STATION_NAME"), Bytes.toBytes(entity.getC_STATION_NAME()));
//			} catch (Exception e) {
//			}
//			try {
//				put.addColumn(family, Bytes.toBytes("C_COUNTY_CODE"), Bytes.toBytes(entity.getC_COUNTY_CODE()));
//			} catch (Exception e) {
//
//			}
//			try {
//				put.addColumn(family, Bytes.toBytes("C_DISTRICTNAME"), Bytes.toBytes(entity.getC_DISTRICTNAME()));
//			} catch (Exception e) {
//			}
//			try {
//				put.addColumn(family, Bytes.toBytes("C_JOBAREA_CODE"), Bytes.toBytes(entity.getC_JOBAREA_CODE()));
//			} catch (Exception e) {
//			}
//			try {
//				put.addColumn(family, Bytes.toBytes("C_STATION_TYPE"), Bytes.toBytes(entity.getC_STATION_TYPE().toString()));
//			} catch (Exception e) {
//			}
//			try {
//				put.addColumn(family, Bytes.toBytes("C_DEVICE_TYPE"), Bytes.toBytes(entity.getC_DEVICE_TYPE()));
//			} catch (Exception e) {
//			}
//			try {
//				put.addColumn(family, Bytes.toBytes("N_ISMAIN"), Bytes.toBytes(entity.getN_ISMAIN().toString()));
//			} catch (Exception e) {
//			}
//			try {
//				put.addColumn(family, Bytes.toBytes("N_TYPE"), Bytes.toBytes(entity.getN_TYPE().toString()));
//			} catch (Exception e) {
//			}
//			try {
//				put.addColumn(family, Bytes.toBytes("ORDER_COLUMN"), Bytes.toBytes(entity.getORDER_COLUMN().toString()));
//			} catch (Exception e) {
//			}
//
//			puts.add(put);
//		}
//		System.out.println(new Date());
//		// HBaseDaoUtil.saveSomePut(puts,"weather_"+date);
//		hbaseTemplate.execute("VIEW_JS_CD_STATION", new TableCallback<String>() {
//
//			@Override
//			public String doInTable(HTableInterface table) throws Throwable {
//				table.put(puts);
//				return null;
//			}
//		});
//		System.out.println(new Date());
//
//		return true;
//	}
//
//}
