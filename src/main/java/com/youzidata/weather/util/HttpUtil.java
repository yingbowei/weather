package com.youzidata.weather.util;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

public class HttpUtil {

	public static void main(String[] args) {
		HttpUtil http =new HttpUtil();
		Map<String, String> headermap = new LinkedHashMap<String, String>();
		String s="http://10.0.252.14:9998/updateData?startDate=2018020400&hour=3&type=HM";
		Map<String,String> map=ll();
		JSONObject json = (JSONObject) JSONObject.toJSON(map);
//		String res=JSONObject.toJSONString(map);
		http.doPost(s, json.toJSONString(), headermap);
	}
	
    public String requestByGetMethod(String url){
    	System.out.println(url);
        //创建默认的httpClient实例
        CloseableHttpClient httpClient = getHttpClient();
        try {
            //用get方法发送http请求
            HttpGet get = new HttpGet(url);
            CloseableHttpResponse httpResponse = null;
            //发送get请求
            httpResponse = httpClient.execute(get);
            String res = null;
            try{
                //response实体
                HttpEntity entity = httpResponse.getEntity();
                if (null != entity){
                	res = EntityUtils.toString(entity);             
                }
            }
            finally{
                httpResponse.close();
            }
            
            return res;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        finally{
            try{
                closeHttpClient(httpClient);
            } catch (IOException e){
                e.printStackTrace();
            }
        }

    }


    /**
     * POST方式发起http请求
     */
    public void requestByPostMethod(String url){
        CloseableHttpClient httpClient = getHttpClient();
        try {
            HttpPost post = new HttpPost(url);
            post.setHeader("Content-type", "application/json;charset=UTF-8");
            //创建参数列表
            List<NameValuePair> list = new ArrayList<NameValuePair>();
            //url格式编码
            UrlEncodedFormEntity uefEntity = new UrlEncodedFormEntity(list,"UTF-8");
            post.setEntity(uefEntity);
            System.out.println("POST 请求...." + post.getURI());
            //执行请求
            CloseableHttpResponse httpResponse = httpClient.execute(post);
            try{
                HttpEntity entity = httpResponse.getEntity();
                if (null != entity){
                    System.out.println("-------------------------------------------------------");
                    System.out.println(EntityUtils.toString(uefEntity));
                    System.out.println("-------------------------------------------------------");
                }
            } finally{
                httpResponse.close();
            }

        } catch( UnsupportedEncodingException e){
            e.printStackTrace();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        finally{
            try{
                closeHttpClient(httpClient);                
            } catch(Exception e){
                e.printStackTrace();
            }
        }

    }
    public JSONObject doPost(String url, String bodyjson,Map<String,String> headerMap) {
        HttpPost post = new HttpPost(url);
        JSONObject response = null;
        try {
             StringEntity s = new StringEntity(bodyjson, "UTF-8"); // 中文乱码在此解决
             s.setContentType("application/json");
             post.setEntity(s);
             Iterator<?> headerIterator = headerMap.entrySet().iterator();          //循环增加header
             while(headerIterator.hasNext()){  
                 Entry<String,String> elem = (Entry<String, String>) headerIterator.next();  
                 post.addHeader(elem.getKey(),elem.getValue());
             }
             HttpResponse res = HttpClients.createDefault().execute(post);
             if (res.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                  String result = EntityUtils.toString(res.getEntity());// 返回json格式：
//                  response = JSON.parseObject(result);
                  System.out.println(result);
             }
        } catch (Exception e) {
             e.printStackTrace();
        }
        return response;
  }
    private CloseableHttpClient getHttpClient(){
        return HttpClients.createDefault();
    }

    private void closeHttpClient(CloseableHttpClient client) throws IOException{
        if (client != null){
            client.close();
        }
    }
    
	public static Map<String,String> ll(){
		String fenge="0.25";
		int cengshu=19;
		Map<String,String> map=new HashMap<>();
		if(fenge.equals("0.125")){
			String lon=",";
			for (double i = 60; i <=150; i=(i+0.125)) {
				if(i==150){
					lon+=i+"\n";
				}else{
					lon+=i+",";
				}
			}
			String s="";
			for (int j = 0; j <=720; j++) {
				double re=Math.random();
				if(j==720){
					s+=re;
				}else{
					s+=re+",";
				}
			}
			String line="";
			for (double i = -10; i <= 60; i=(i+0.125)) {
				String i1=(i+"");
				line+=i1+","+s+"\n";
			}
			System.out.println(lon+line);
			map.put(String.valueOf(0), lon+line);
		}else{
			String lon=",";
			for (double i = 60; i <=150; i=(i+0.25)) {
				if(i==150){
					lon+=i+"\n";
				}else{
					lon+=i+",";
				}
			}
			for (int z = 1; z <=cengshu; z++) {
				String s="";
				for (int j = 0; j <=360; j++) {
					double re=Math.random();
					if(j==360){
						s+=re;
					}else{
						s+=re+",";
					}
				}
				String line="";
				for (double i = -10; i <= 60; i=(i+0.25)) {
					String i1=(i+"");
					line+=i1+","+s+"\n";
				}
				System.out.println(lon+line);
				map.put(String.valueOf(z), lon+line);
			}
		}
		return map;
		
		
	}
}
