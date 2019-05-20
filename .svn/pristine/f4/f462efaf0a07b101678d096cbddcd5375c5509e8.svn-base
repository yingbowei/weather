package com.youzidata.weather.task;

import ucar.nc2.Dimension;
import ucar.nc2.Variable;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

public class ParseTwoThread implements Callable<String> {

    private Variable n;
    private String lon;
    private float[] lat;
    float[][] resultData;
    public ParseTwoThread(float[][] data,String lonstr,float[] latarr){
//        this.n = v;
        this.resultData = data;
        this.lon = lonstr;
        this.lat = latarr;
    }

    @Override
    public String call() throws Exception {
        long time = System.currentTimeMillis();
//        String twostr = parseFileTwo(lonstr,latarr, variable);
//        return twostr;

        StringBuilder keystr = new StringBuilder();
        keystr.append(lon);

        for(int i=0;i<resultData.length;i++){
            keystr.append(lat[i]);
//            keystr.append(Arrays.toString(resultData[i]).replace("[","").replace("]",""));
            for(int l=0;l<resultData[i].length;l++){
                keystr.append(",");
                keystr.append(resultData[i][l]);
            }
            keystr.append("\n");
        }
//         System.out.println("==========要素解析时间======"+(System.currentTimeMillis() - time));
        return keystr.toString();
    }

    public String parseFileTwo(String lon, float[] lat, Variable n){

        StringBuilder keystr = new StringBuilder();
        keystr.append(lon);

        try {
            System.out.println(n.getShortName()+n.getDimensions().size());
            int[] org = new int[n.getDimensions().size()];
            int[] msg = new int[n.getDimensions().size()];
            for(int i=0;i<n.getDimensions().size();i++) {
                org[i] = 0;
                msg[i] = n.getDimensions().get(i).getLength();
            }
            float[][] resultData = new float[msg[0]][msg[1]];
            int length = msg[0];
            msg[0] = 1;
            //分层读取文件  具体分的层数  目前是取决于第一维度的大小
            float[][] data = (float[][])n.read(org, msg).copyToNDJavaArray();
            for(int i=0;i<length;i++) {
                keystr.append(lat[i]);
                for(int l=0;l<data[0].length;l++){
                    keystr.append(",");
                    keystr.append(data[0][l]);
                }
                keystr.append("\n");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return keystr.toString();
    }
}
