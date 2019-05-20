package com.youzidata.weather.task;

import com.youzidata.weather.util.ConversionUtil;
import ucar.nc2.Dimension;
import ucar.nc2.Variable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

public class ParseThreeFloatThread implements Callable<List<byte[]>> {

    private Variable n;
    private String lon;
    private float[] lat;
    private float[][][] resultData;
    public ParseThreeFloatThread(float[][][] data, String lonstr, float[] latarr){
//        this.n = v;
        this.lon = lonstr;
        this.lat = latarr;
        this.resultData = data;
    }

    @Override
    public List<byte[]> call() throws Exception {
        long time = System.currentTimeMillis();
        List<byte[]> res = new ArrayList<byte[]>();

        for(int i=0;i<resultData.length;i++) {
            byte[] bytes = new byte[resultData[0].length*resultData[0][0].length*4];
            for(int l=0;l<resultData[i].length;l++) {
                byte[] byte1 = ConversionUtil.floatToBytes(resultData[i][l]);
                System.arraycopy(byte1,0,bytes,i*resultData[0][0].length*4,byte1.length);
            }
            res.add(bytes);
        }
        return res;
    }

}
