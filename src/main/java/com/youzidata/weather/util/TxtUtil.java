package com.youzidata.weather.util;

import org.jcodings.util.Hash;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Nickwong on 31/07/2018.
 * 根据1-8楼的建议，优化了代码
 */
public class TxtUtil {

    public static void main(String args[]) {
//        String path = "C:\\Users\\yingbowei\\Desktop\\test";
//        String content = "GRID_TJQX_PUB_DIS_AFTJ_000_DT_20190506080000_000-072_401.nc";
//        String date = StringUtil.getDateStrFromNc(content, 8);
//        String allPath = path + "\\" + date + ".txt";
//        writeFile(allPath, content);
//        List<String> fileStrList = readFile2(allPath);
//        System.out.println(fileStrList.size());
    }

    /**
     * 读入TXT文件
     */
    public static String readFile(String path) {
        String pathname = path; // 绝对路径或相对路径都可以，写入文件时演示相对路径,读取以上路径的input.txt文件
        StringBuffer sBuffer = new StringBuffer();
        //防止文件建立或读取失败，用catch捕捉错误并打印，也可以throw;
        //不关闭文件会导致资源的泄露，读写文件都同理
        //Java7的try-with-resources可以优雅关闭文件，异常时自动关闭文件；详细解读https://stackoverflow.com/a/12665271
        try (FileReader reader = new FileReader(pathname);
             BufferedReader br = new BufferedReader(reader) // 建立一个对象，它把文件内容转成计算机能读懂的语言
        ) {
            String line;
            //网友推荐更加简洁的写法
            while ((line = br.readLine()) != null) {
                // 一次读入一行数据
                sBuffer.append(line);
//                System.out.println(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return sBuffer.toString();
    }

    /**
     * 读取txt文件，将每行存入集合中返回
     * @param path
     * @return
     */
    public static Map<String, String> readFile2(String path) {
        Map<String, String> result = new HashMap<>();
        File file = new File(path);
        if(!file.exists()) {
            return result;
        }
//        StringBuffer sBuffer = new StringBuffer();
        //防止文件建立或读取失败，用catch捕捉错误并打印，也可以throw;
        //不关闭文件会导致资源的泄露，读写文件都同理
        //Java7的try-with-resources可以优雅关闭文件，异常时自动关闭文件；详细解读https://stackoverflow.com/a/12665271
        try (FileReader reader = new FileReader(path);
             BufferedReader br = new BufferedReader(reader) // 建立一个对象，它把文件内容转成计算机能读懂的语言
        ) {
            String line;
            //网友推荐更加简洁的写法
            while ((line = br.readLine()) != null) {
                // 一次读入一行数据
                result.put(line, "");
//                System.out.println(line);
            }
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    /**
     * linux操作系统下，追加写入txt文件
     * @param filePath
     * @param content
     */
    public static void writeFile(String filePath, String content) {
        try {
            File writeName = new File(filePath); // 相对路径，如果没有则要建立一个新的文件
            if(!writeName.exists()) {
                writeName.createNewFile(); // 如果文件不存在，创建新文件
            }
            try (FileWriter writer = new FileWriter(writeName, true);
                 BufferedWriter out = new BufferedWriter(writer)
            ) {
//                out.append(content + "\r\n");
                out.write(content + System.getProperty("line.separator")); //通用换行代码
                out.flush(); // 把缓存区内容压入文件
                out.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}


