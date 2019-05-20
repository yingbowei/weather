package com.youzidata.weather.task;

import ucar.nc2.Dimension;
import ucar.nc2.Variable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

public class ParseThreeThread implements Callable<List<String>> {

    private Variable n;
    private String lon;
    private float[] lat;
    private float[][][] resultData;
    public ParseThreeThread(float[][][] data,String lonstr,float[] latarr){
//        this.n = v;
        this.lon = lonstr;
        this.lat = latarr;
        this.resultData = data;
    }

    @Override
    public List<String> call() throws Exception {
//        List<String> threelist = parseFileThree(lonstr,latarr,variable.getDimensions(), variable);
//        return threelist;
        long time = System.currentTimeMillis();
        List<String> res = new ArrayList<String>();

        for(int i=0;i<resultData.length;i++) {
            StringBuilder keystr = new StringBuilder();
            keystr.append(lon);
            for(int l=0;l<resultData[i].length;l++) {
                keystr.append(lat[l]);
//                keystr.append(Arrays.toString(resultData[i][l]).replace("[","").replace("]",""));
                for(int p =0;p<resultData[i][l].length;p++) {
                    keystr.append(",");
                    keystr.append(resultData[i][l][p]);
                }
                keystr.append("\n");
            }
            res.add(keystr.toString());
        }
//        System.out.println("==========要素解析时间======"+(System.currentTimeMillis() - time));
        return res;
    }


    public List<String> parseFileThree(String lon, float[] lat, List<Dimension> dimensions,Variable n){

        List<String> res = new ArrayList<String>();
        try {

            int[] org = new int[3];
            int[] msg = new int[3];

            for(int i=0;i<dimensions.size();i++) {
                org[i] = 0;
                msg[i] = dimensions.get(i).getLength();
            }
            float[][][] resultData = new float[msg[0]][msg[1]][msg[2]];
            int length = msg[0];
            msg[0] = 1;
            //分层读取文件  具体分的层数  目前是取决于第一维度的大小
            for(int i=0;i<length;i++) {
                org[0] = i;
                float[][][] data = (float[][][])n.read(org, msg).copyToNDJavaArray();
                StringBuilder keystr = new StringBuilder();
                keystr.append(lon);
                for(int l=0;l<data[0].length;l++){
                    keystr.append(lat[l]);
                    for(int p =0;p<data[0][l].length;p++) {
                        keystr.append(",");
                        keystr.append(data[0][l][p]);
                    }
                    keystr.append("\n");
                }

                res.add(keystr.toString());
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return res;
    }
}
