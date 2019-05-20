package com.youzidata.weather.task;

import com.youzidata.weather.util.ConversionUtil;
import ucar.nc2.Variable;

import java.util.concurrent.Callable;

public class ParseTwoFloatThread implements Callable<byte[]> {

    private Variable n;
    private String lon;
    private float[] lat;
    float[][] resultData;
    public ParseTwoFloatThread(float[][] data, String lonstr, float[] latarr){
//        this.n = v;
        this.resultData = data;
        this.lon = lonstr;
        this.lat = latarr;
    }

    @Override
    public byte[] call() throws Exception {
        long time = System.currentTimeMillis();

        byte[] bytes = new byte[resultData.length*resultData[0].length*4];

        for(int i=0;i<resultData.length;i++){
            byte[] byte1 = ConversionUtil.floatToBytes(resultData[i]);
            System.arraycopy(byte1,0,bytes,i*resultData[0].length*4,byte1.length);
        }

        return bytes;
    }

}
