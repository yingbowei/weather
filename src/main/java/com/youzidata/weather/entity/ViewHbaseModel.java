package com.youzidata.weather.entity;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class ViewHbaseModel {
	public static List<Map<String, String>> list = new ArrayList<Map<String, String>>();

	public static String rowkey;

	static{
		rowkey="V01000";
		list.add(setFieldAttribute("V01000", "V01000", "cf"));
		list.add(setFieldAttribute("V05001", "V05001", "cf"));
		list.add(setFieldAttribute("V06001", "V06001", "cf"));
		list.add(setFieldAttribute("V07001", "V07001", "cf"));
		list.add(setFieldAttribute("C_STATION_NAME", "C_STATION_NAME", "cf"));
		list.add(setFieldAttribute("C_COUNTY_CODE", "C_COUNTY_CODE", "cf"));
		list.add(setFieldAttribute("C_DISTRICTNAME", "C_DISTRICTNAME", "cf"));
		list.add(setFieldAttribute("C_JOBAREA_CODE", "C_JOBAREA_CODE", "cf"));
		list.add(setFieldAttribute("C_STATION_TYPE", "C_STATION_TYPE", "cf"));
		list.add(setFieldAttribute("C_DEVICE_TYPE", "C_DEVICE_TYPE", "cf"));
		list.add(setFieldAttribute("N_ISMAIN", "N_ISMAIN", "cf"));
		list.add(setFieldAttribute("N_TYPE", "N_TYPE", "cf"));
		list.add(setFieldAttribute("ORDER_COLUMN", "ORDER_COLUMN", "cf"));
	}

	/**
	 *
	 * @param field es字段
	 * @param name	消息字段名
	 * @param family	es字段类型
	 * @return
	 */
	private static Map<String, String> setFieldAttribute(String field,String name,String family){
		Map<String, String> map = new HashMap<String, String>();

		map.put("field", field);
		map.put("name", name);
		map.put("family", family);

		return map;
	}

	/**
	 * 设置rowkey，rowkey为mmsi字段的值
	 * @param map
	 * @return
	 */
	public String getRowkey(Map<String,Object> map){
		return map.get("V01000").toString();
	}

	public Boolean fliterData(Map<String, Object> map){
		if(map.containsKey("V01000")){
			return true;
		}
		return false;
	}

}
