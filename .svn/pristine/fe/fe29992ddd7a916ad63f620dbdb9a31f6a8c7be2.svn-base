package com.youzidata.weather.util;

import org.apache.commons.lang.StringUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.util.*;
import java.util.regex.Pattern;

/**
 * @Author: YingBoWei
 * @Date: 2019-05-07 16:24
 * @Description:
 */
public class FileUtil {
    public static void main(String[] args) {
    }

    /**
     * 单要素多时效
     * 入库去掉用户id，相同文件不同用户id的选择最新修改时间的文件入库
     * @param dirPath
     * @param startDate
     * @param endDate
     * @return
     */
    public static List<File> getNewestFileAndNoUserId(String dirPath, String startDate, String endDate) {
        //获取所有包含指定日期范围，所有用户id的nc文件
        List<File> fileList = getFileListFromOneFactor(dirPath, startDate, endDate);
        List<File> result = new ArrayList();
        Map<String, List<File>> fileMap = new HashMap();
        for(File file:fileList) {
            String fileName = file.getName();
            String[] strArr =fileName.split("_");
            String[] newStrArr = new String [strArr.length - 1];
            System.arraycopy(strArr, 0, newStrArr, 0, newStrArr.length);
            String key = StringUtils.join(newStrArr, "_");
            if(fileMap.containsKey(key)) {
                fileMap.get(key).add(file);
            }else {
                List<File> list = new ArrayList<>();
                list.add(file);
                fileMap.put(key, list);
            }
        }
        for(Map.Entry<String, List<File>> m:fileMap.entrySet()) {
            List<File> list = m.getValue();
            Map<String, File> file_map = new HashMap();
            for(File file:list) {
                String time = getModifyTimeFromCentos(file.getPath());
                file_map.put(time, file);
            }
            Set<String> set = file_map.keySet();
            Object[] obj = set.toArray();
            Arrays.sort(obj);
            result.add(file_map.get(obj[obj.length - 1]));
        }
        return result;
    }


    /**
     * 单要素多时效
     * 获取当前目录中符合日期要求的所有nc文件
     * @param dirPath
     * @param startDate
     * @param endDate
     * @return
     */
    public static List<File> getFileListFromOneFactor(String dirPath, String startDate, String endDate) {
        File file = new File(dirPath);
        List<String> date = DateUtil.getDayRangeStrList(startDate, endDate, "yyyyMMdd", "yyyyMMdd");
        List<File> listDirFiles = new ArrayList<>();
        fileOperationSingleEle(file, date, listDirFiles);
        List<File> listFiles = new ArrayList<>();
        for(File f:listDirFiles) {
            fileOperation(f, listFiles);
        }
        return listFiles;
    }

    /**
     * 获取符合单要素多时效文件目录结构的所有日期范围内的目录
     * @param file
     * @param date
     * @param listDirFiles
     */
    public static void fileOperationSingleEle(File file, List<String> date, List<File> listDirFiles) {
        String a1 = "^(19|20)[0-9]{8}$";//yyyyMMddHH,不适用于所有的年月日小时判断，这里只做基础的数字判断
        String a2 = "^(19|20)[0-9]{6}$";//yyyyMMdd
        String a3 = "^(19|20)[0-9]{4}$";//yyyyMM
        String a4 = "^(19|20)[0-9]{2}$";//yyyy
        try {
            if (file.isDirectory() && !Pattern.matches(a1, file.getName())) {//是目录，而且不是文件名不属于yyyyMMddHH结构
                File[] files = file.listFiles();
                for (File f : files) {
                    String f_name = f.getName();
                    if(Pattern.matches(a2, f_name) && inArr2(f_name, date)) {
                        listDirFiles.add(f);
                    }else {
                        fileOperationSingleEle(f, date, listDirFiles);
                    }
                }
            } else {
                // do sommethings ......
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 通用方法，递归获取目录中的所有文件
     * @param file
     * @param fileList
     */
    public static void fileOperation(File file, List<File> fileList) {
        try {
            if (file.isDirectory()) {
                File[] files = file.listFiles();
                for (File f : files) {
                    fileOperation(f, fileList);
                }
            } else {
                // do sommethings ......
                fileList.add(file);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

//    /**
//     * 单要素多时效的nc文件
//     * 获取单个要素文件夹内，指定时间范围内的文件路径
//     * @param dirPath   目录路径
//     * @param date  查询的时间范围
//     * @return
//     */
//    public static List<File> getFileListFromOneFactor(String dirPath, List<String> date) {
//        List<File> list = new ArrayList<>();
//        File dirFile = new File(dirPath);
//        List<File> dirFiles = Arrays.asList(dirFile.listFiles());
//        boolean flag = true;
//        while(flag && dirFiles != null && dirFiles.size() > 0) {
//            List<File> dirFilesTemp = new ArrayList();
//            if(dirFiles.get(0).listFiles()[0].isDirectory()) {//当前目录的下一级目录如果还是目录，则执行
//                for(File file:dirFiles) {
//                    if(inArr(file.getName(), date)) {
//                        File[] _dirFiles = file.listFiles();
//                        if(_dirFiles != null && _dirFiles.length > 0) {
//                            for(File _file:_dirFiles) {
//                                dirFilesTemp.add(_file);
//                            }
//                        }
//                    }
//                }
//                dirFiles = dirFilesTemp;
//            }else {
//                flag = false;
//            }
//        }
//
//        if(dirFiles != null && dirFiles.size() > 0) {
//            for(File file:dirFiles) {
//                List<File> _files = Arrays.asList(file.listFiles());
//                list.addAll(_files);
//            }
//        }
//        return list;
//    }
//
//    public static boolean inArr(String str, List<String> list) {
//        for(String li:list) {
//            String y = li.substring(0,4);
//            String ym = li.substring(0,6);
//            String ymd = li.substring(0,8);
//            if(str.indexOf(y) != -1 || str.indexOf(ym) != -1 || str.indexOf(ymd) != -1) {
//                return true;
//            }
//        }
//        return false;
//    }

    /**
     * 判断文件名是否满足日期格式
     * @param str   用来比对的文件名
     * @param list  需要入库的日期集合，yyyyMMdd
     * @return
     */
    public static boolean inArr2(String str, List<String> list) {
        for(String li:list) {
            String y = li.substring(0,4);
            String ym = li.substring(0,6);
            String ymd = li.substring(0,8);
            if(str.equals(y) || str.equals(ym) || str.equals(ymd)) {
                return true;
            }
        }
        return false;
    }



//    public static List<File> filelist = new ArrayList<>();//保存递归结果
//    /**
//     * 递归获取某一文件目录下的所有nc文件
//     * @param strPath
//     * @return
//     */
//    public static List<File> getFileList(String strPath) {
////        filelist = new ArrayList<>();
//        File dir = new File(strPath);
//        File[] files = dir.listFiles(); // 该文件目录下文件全部放入数组
//        if (files != null) {
//            for (int i = 0; i < files.length; i++) {
//                String fileName = files[i].getName();
//                if (files[i].isDirectory()) { // 判断是文件还是文件夹
//                    getFileList(files[i].getAbsolutePath()); // 获取文件绝对路径
//                } else if (fileName.endsWith(".nc") && fileName.split("_").length == 10) { // 判断文件名是否以.avi结尾
//                    filelist.add(files[i]);
//                } else {
//                    continue;
//                }
//            }
//
//        }
//        return filelist;
//    }

    /**
     * 获得当前目录中的所有nc文件
     * @param dirPath
     * @return
     */
    public static List<File> getFilelist2(String dirPath) {
        List<File> result = new ArrayList<>();
        File dir = new File(dirPath);
        File[] files = dir.listFiles(); // 该文件目录下文件全部放入数组
        for(File file:files) {
            String fileName = file.getName();
            if(fileName.endsWith(".nc") && fileName.split("_").length == 10) {
                result.add(file);
            }
        }
        return result;
    }

    /**
     * 获取centos上面文件的修改时间
     * @param filePath
     * @return
     */
    public static String getModifyTimeFromCentos(String filePath) {
        String time = null;
        try {
            String[] cmd = new String[]{"stat", "-c", "%Y", filePath};
            Process ps = Runtime.getRuntime().exec(cmd);

            BufferedReader br = new BufferedReader(new InputStreamReader(ps.getInputStream()));
            StringBuffer sb = new StringBuffer();
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line).append("\n");
            }
            String result = sb.toString();
            time = result;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return time;
    }
}
