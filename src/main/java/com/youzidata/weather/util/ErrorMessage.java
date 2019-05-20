package com.youzidata.weather.util;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Jiarong Xu
 * @date Aug 7, 2014 5:27:31 PM
 *
 */

public class ErrorMessage {

	protected static Map<String, String> ERROR_MAP = new HashMap<String, String>();
	
	public static String LAT_OR_LON_ERROR    = "1001";
	
	static {
		ERROR_MAP.put(LAT_OR_LON_ERROR, "经纬度填写不正确，经度范围：60.0~240.0，纬度范围：-90.0~90.0");
	}
	
	public static String getMessage(String code) {
		return ERROR_MAP.get(code);
	}
}
