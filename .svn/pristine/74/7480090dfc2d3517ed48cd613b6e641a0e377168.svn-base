package com.youzidata.weather.util;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.slf4j.LoggerFactory;

public class ZipUtil {
	 
//    private static Logger logger = LoggerFactory.getLogger(ZipUtils.class);

    // 目录标识判断符
    public static final String PATCH = "/";
    // 基目录
    public static final String BASE_DIR = "";
    // 缓冲区大小
    private static final int BUFFER = 2048;
    // 字符集
//    public static final String CHAR_SET = "GBK";


    /**
     *
     * 描述: 压缩文件
     * @author wanghui
     * @created 2017年10月27日
     * @param fileOutName
     * @param files
     * @throws Exception
     */
    public static void compress(String fileOutName, List<File> files) throws Exception {
        try {
            FileOutputStream fileOutputStream = new FileOutputStream(fileOutName);
            ZipOutputStream zipOutputStream = new ZipOutputStream(fileOutputStream);

            if (files != null && files.size() > 0) {
                for (int i = 0,size = files.size(); i < size; i++) {
                    compress(files.get(i), zipOutputStream, BASE_DIR);
                }
            }
            // 冲刷输出流
            zipOutputStream.flush();
            // 关闭输出流
            zipOutputStream.close();
        } catch (Exception e) {
            throw new Exception(e.getMessage(),e);
        }
    }



    /**
     *
     * 描述: 压缩
     * @author wanghui
     * @created 2017年10月27日
     * @param srcFile
     * @param zipOutputStream
     * @param basePath
     * @throws Exception
     */
    public static void compress(File srcFile, ZipOutputStream zipOutputStream, String basePath) throws Exception {
        if (srcFile.isDirectory()) {
            compressDir(srcFile, zipOutputStream, basePath);
        } else {
            compressFile(srcFile, zipOutputStream, basePath);
        }
    }

    /**
     *
     * 描述:压缩目录下的所有文件
     * @author wanghui
     * @created 2017年10月27日
     * @param dir
     * @param zipOutputStream
     * @param basePath
     * @throws Exception
     */
    private static void compressDir(File dir, ZipOutputStream zipOutputStream, String basePath) throws Exception {
        try {
            // 获取文件列表
            File[] files = dir.listFiles();

            if (files.length < 1) {
                ZipEntry zipEntry = new ZipEntry(basePath + dir.getName() + PATCH);

                zipOutputStream.putNextEntry(zipEntry);
                zipOutputStream.closeEntry();
            }

            for (int i = 0,size = files.length; i < size; i++) {
                compress(files[i], zipOutputStream, basePath + dir.getName() + PATCH);
            }
        } catch (Exception e) {
            throw new Exception(e.getMessage(), e);
        }
    }

    /**
     *
     * 描述:压缩文件
     * @author wanghui
     * @created 2017年10月27日
     * @param file
     * @param zipOutputStream
     * @param dir
     * @throws Exception
     */
    private static void compressFile(File file, ZipOutputStream zipOutputStream, String dir) throws Exception {
        try {
            // 压缩文件
            ZipEntry zipEntry = new ZipEntry(dir + file.getName());
            zipOutputStream.putNextEntry(zipEntry);

            // 读取文件
            BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file));

            int count = 0;
            byte data[] = new byte[BUFFER];
            while ((count = bis.read(data, 0, BUFFER)) != -1) {
                zipOutputStream.write(data, 0, count);
            }
            bis.close();
            zipOutputStream.closeEntry();
        } catch (Exception e) {
            throw new Exception(e.getMessage(),e);
        }
    }
}

