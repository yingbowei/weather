package com.youzidata.weather.controller;

import com.youzidata.weather.service.HbaseService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@RestController
@CrossOrigin
public class UpdateWeatherInfoController {

    @Autowired
    HbaseService hbaseService;

    @PostMapping(value="/updateTest")
    public Object updateTest(@RequestParam(required=true)String startDate, @RequestParam(required=true) Integer hour
            , @RequestParam(required=true) String type
            , @RequestParam(required=true) Integer layer
            , @RequestParam(required=true) String interval) throws IOException {
    	return hbaseService.updateDataToHbaseTest(startDate,hour,type,layer,interval);
    }
    
    @PostMapping(value="/updateData")
    public Object updateData(@RequestParam(required=true)String startDate, @RequestParam(required=true) Integer hour
            , @RequestParam(required=true) String type
            , @RequestBody(required=true) Map<String,String> map )
            		throws IOException {
    	return hbaseService.updateDataToHbase(startDate,hour,type,map);
    }
}
