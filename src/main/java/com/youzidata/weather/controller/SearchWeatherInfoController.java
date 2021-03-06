package com.youzidata.weather.controller;

import com.youzidata.weather.service.HbaseService;
import com.youzidata.weather.service.HbaseService1;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author: YingBoWei
 * @Date: 2019-04-17 10:28
 * @Description:    单要素多时效数据查询
 */
@RestController
@CrossOrigin
public class SearchWeatherInfoController {

	@Autowired
	HbaseService hbaseService;

	@Autowired
	HbaseService1 hbaseService1;

	// @ResponseBody
	// @GetMapping(value="/searchWeatherInfo")
	// public Object test(@RequestParam(required=true)String
	// startDate,@RequestParam(required=true)Integer hour
	// ,@RequestParam(required=true)String type
	// ,@RequestParam(required=false)Integer decimal
	// ,@RequestParam(required=true) List<String> layer
	// ,@RequestParam(required=false)Double startLat
	// ,@RequestParam(required=false)Double endLat
	// ,@RequestParam(required=false)Double startLon
	// ,@RequestParam(required=false)Double endLon) {
	//
	// List<String> lat0lon1 = Arrays.asList("SFTMP");
	//
	// if(startLat!=null && !startLat.equals("")){
	// if(lat0lon1.contains(type)){
	// if(startLat<-10){
	// startLat=(double) -10;
	// }
	// if(endLat>60){
	// endLat=(double) 60;
	// }
	// if(startLon<60){
	// startLon=(double) 60;
	// }
	// if(endLon>180){
	// endLon=(double) 180;
	// }
	// }else{
	// if(startLat<-10){
	// startLat=(double) -10;
	// }
	// if(endLat>60){
	// endLat=(double) 60;
	// }
	// if(startLon<60){
	// startLon=(double) 60;
	// }
	// if(endLon>150){
	// endLon=(double) 150;
	// }
	// }
	// }
	//
	//
	//
	// if(type.equalsIgnoreCase("700-500TMP")){
	// type="TMP1";
	// }else if(type.equalsIgnoreCase("850-500TMP")){
	// type="TMP2";
	// }
	// Map<String, Object> resultMap =
	// hbaseService.getDateFromHbase(startDate,hour,type,decimal,layer,startLat,endLat,startLon,endLon);
	//
	// if(resultMap != null) {
	// resultMap.put("code", "200");
	// }else {
	// resultMap = new HashMap<>();
	// resultMap.put("code", "10001");
	// resultMap.put("ERROR", "数据为null");
	// return resultMap;
	// }
	// return resultMap;
	// }

	@ResponseBody
	@GetMapping(value = "/searchWeatherInfo")
	public Object test1(@RequestParam(required = true) String startDate, @RequestParam(required = true) Integer hour,
			@RequestParam(required = true) String type, @RequestParam(required = false) Integer decimal,
			@RequestParam(required = true) List<String> layer, @RequestParam(required = false) Double startLat,
			@RequestParam(required = false) Double endLat, @RequestParam(required = false) Double startLon,
			@RequestParam(required = false) Double endLon) {

		List<String> lat0lon1 = Arrays.asList("CAPE","CP","DEPR2M","EDA10M","H0C","LCC","M","MV","PRMSL","RH2M","SFPRES","SFTMP","SNOW","SNPWDPT","TCC","TD2M","TMP2M","TP","VIS","VV10M");

		if (startLat != null && !startLat.equals("")) {
			if (lat0lon1.contains(type)) {
				if (startLat < -10) {
					startLat = (double) -10;
				}
				if (endLat > 60) {
					endLat = (double) 60;
				}
				if (startLon < 60) {
					startLon = (double) 60;
				}
				if (endLon > 180) {
					endLon = (double) 180;
				}
			} else {
				if (startLat < -10) {
					startLat = (double) -10;
				}
				if (endLat > 60) {
					endLat = (double) 60;
				}
				if (startLon < 60) {
					startLon = (double) 60;
				}
				if (endLon > 150) {
					endLon = (double) 150;
				}
			}
		}

//		if (type.equalsIgnoreCase("700-500TMP")) {
//			type = "TMP1";
//		} else if (type.equalsIgnoreCase("850-500TMP")) {
//			type = "TMP2";
//		}
		Map<String, Object> resultMap = new HashMap<>();
		if("EDA".equals(type)) {
			resultMap = hbaseService1.getDateFromHbaseTgz2(startDate, hour, type, decimal, layer, startLat,
					endLat, startLon, endLon);
		}else {
			resultMap = hbaseService1.getDateFromHbaseTgz(startDate, hour, type, decimal, layer, startLat,
					endLat, startLon, endLon);
		}

		if (resultMap != null) {
			resultMap.put("code", "200");
		} else {
			resultMap = new HashMap<>();
			resultMap.put("code", "10001");
			resultMap.put("ERROR", "数据为null");
			return resultMap;
		}
		return resultMap;
	}

	@ResponseBody
	@GetMapping(value = "/searchWeatherOne")
	public Object test2(@RequestParam(required = true) String startDate, @RequestParam(required = true) Integer hour,
			@RequestParam(required = true) String type, @RequestParam(required = false) Integer decimal,
			@RequestParam(required = true) List<String> layer, @RequestParam(required = true) Double lat,
			@RequestParam(required = true) Double lon) {

		if (type.equalsIgnoreCase("700-500TMP")) {
			type = "TMP1";
		} else if (type.equalsIgnoreCase("850-500TMP")) {
			type = "TMP2";
		}
		Object list = hbaseService1.getDateFromHbaseOne(startDate, hour, type, decimal, layer, lat,
				lon);
		Map<String, Object> resultMap =new LinkedHashMap<>();
		if (list != null) {
			resultMap.put("code", "200");
			resultMap.put("list", list);
		} else {
			resultMap = new HashMap<>();
			resultMap.put("code", "10001");
			resultMap.put("ERROR", "数据为null");
			return resultMap;
		}
		return resultMap;
	}
}
