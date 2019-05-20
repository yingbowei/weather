package com.youzidata.weather.controller;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URLEncoder;
import java.text.DecimalFormat;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * 
 * 类名称：DownLoadController   
 * 类描述：   文件下载
 * 创建人：manhaiying
 * 创建时间：2018年12月24日 上午8:59:20      
 * @version  V1.0
 *
 */
@RestController
@CrossOrigin
public class DownLoadController {
	@Value("${CSVPath}")
	private  String CSVPath;
	@Value("${IPADDR}")
	private String IPADDR;
	
	@ResponseBody
	@GetMapping(value="/loadFile")
//	@ApiImplicitParam(name = "fileName",paramType = "query",value = "文件名称",dataType = "string")
	public  Object downFile(HttpServletRequest request,HttpServletResponse response,
			@RequestParam(required=true)String fileName){
		System.out.println("============开始下载==========");
		long startTime = System.currentTimeMillis();
//		String fileName = request.getParameter("file");
		fileName = fileName+".tgz";
		String filePath = CSVPath+fileName;
		File file = new File(filePath);
		 if (fileName != null) {
	            //设置文件路径
	            // 如果文件名存在，则进行下载
	            if (file.exists()) {
	
	                FileInputStream fis = null;
	                BufferedInputStream bis = null;
	                OutputStream os = null;
	                try {
	                	 // 配置文件下载
		                response.setHeader("content-type", "application/octet-stream");
		                response.setContentType("application/octet-stream");
		                // 下载文件能正常显示中文
//		                response.setCharacterEncoding("UTF-8");
		                response.setHeader("Content-Disposition", "attachment;filename=" + URLEncoder.encode(fileName, "UTF-8"));
		                response.setContentLength((int) file.length());
		                // 实现文件下载
		                byte[] buffer = new byte[1024];
	                    fis = new FileInputStream(file);
	                    bis = new BufferedInputStream(fis);
	                    os = response.getOutputStream();
	                    int i = -1;
	                    while ((i = bis.read(buffer)) != -1) {
	                        os.write(buffer, 0, i);
	                    }
	                   os.flush();
	                    System.out.println("Download the file successfully!");
	                }
	                catch (Exception e) {
	                    System.out.println("Download the file failed!");
	                }
	                finally {
	                    if (bis != null) {
	                        try {
	                            bis.close();
	                        } catch (IOException e) {
	                            e.printStackTrace();
	                        }
	                    }
	                    if (fis != null) {
	                        try {
	                            fis.close();
	                        } catch (IOException e) {
	                            e.printStackTrace();
	                        }
	                    }
	                    if (os != null) {
	                        try {
	                        	os.flush();
	                        	os.close();
	                        } catch (IOException e) {
	                            e.printStackTrace();
	                        }
	                    }
	                }
	            }
	        }
		 System.out.println("========================下载结束======================");
		 long endTime = System.currentTimeMillis();
		 System.out.println("下载共消耗======================="+(endTime-startTime));
		return null;
	}
}
