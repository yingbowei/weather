package com.youzidata.weather;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling //启动定时任务
@ComponentScan("com.youzidata.weather")
//@EnableSwagger2
public class WeatherHbaseApplication {

    public static void main(String[] args) {
		SpringApplication.run(WeatherHbaseApplication.class, args);
//		LoadToHbaseTask loadToHbaseTask = new LoadToHbaseTask();
//		loadToHbaseTask.test("D:\\data\\weather");
//		loadToHbaseTask.test(args[0]);
	}



}
