package com.youzidata.weather.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/*
 * 检查参数
 */
public class CheckParamUtil {
	
	public static Map<String,String> getErrorMap(String code){
		Map<String,String> map = new HashMap<String,String>();
		map.put("resultcode", code);
		map.put("reason", ErrorMessage.getMessage(code));
		return map;
	}
	
	public static Map<String,Object> getsuccess(Object data){
		Map<String,Object> map = new LinkedHashMap<String,Object>();
		map.put("resultcode", "200");
		map.put("reason", "成功");
		map.put("data", data);
		return map;
	}
	
	public static Map<String,Object> getsqlsuccess(){
		Map<String,Object> map = new LinkedHashMap<String,Object>();
		map.put("resultcode", "200");
		map.put("reason", "成功");
		return map;
	}
}
