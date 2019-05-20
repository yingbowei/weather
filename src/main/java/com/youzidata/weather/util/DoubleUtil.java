package com.youzidata.weather.util;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;


/**
 * @author ZhangZhiWu
 * @version 创建时间：2019年1月14日 下午5:05:08
 * 
 */
public class DoubleUtil {
	
	/*
	 * a为保留几位小数
	 * b为用逗号分隔的字符串
	 */
	public String doubleDecimal(int a, String[] b) {
		List<Double> lidouble=new ArrayList<>();
		for (String string : b) {
			BigDecimal bg = new BigDecimal(Double.parseDouble(string));
			double f1 = bg.setScale(a, BigDecimal.ROUND_HALF_UP).doubleValue();
			lidouble.add(f1);
		}
		return StringUtils.join(lidouble,",");
	}
	
}
