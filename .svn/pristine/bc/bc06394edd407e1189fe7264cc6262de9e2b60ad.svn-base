package com.youzidata.weather.util;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
 
//import org.apache.log4j.Logger;
 
public class ConversionUtil {
//	private static Logger logger = Logger.getLogger(test.class);
	
	
	//小端 LITTLE_ENDIAN   大端BIG_ENDIAN
	public static float[] bytesToFloat(byte[] bytes) {
		if(bytes==null){
			return null;
		}
		float[] floats = new float[bytes.length/4];
		ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer().get(floats);
		
	    return floats;
	}
	public static byte[] floatToBytes(float[] floats) {
		if(floats==null){
			return null;
		}
		byte[] bytes = new byte[floats.length * 4];
		ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer().put(floats);
	    return bytes;
	}	

}
