package com.youzidata.weather;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.File;
import java.math.BigDecimal;
import java.util.Map;
import java.util.TreeMap;


@RunWith(SpringRunner.class)
@SpringBootTest
public class WeatherHbaseApplicationTests {

	@Test
	public void contextLoads() throws InterruptedException {
		String path = "/test/test/GRID_TJQX_PUB_DIS_AFTJ_000_DT_20180101080000_000-072_601.nc";
		File file = new File(path);
		String name = file.getName();
		String [] strArr = name.split("_");

		System.out.println(strArr[7].substring(0,10));
		System.out.println(strArr[8].split("-")[1]);
	}

}
