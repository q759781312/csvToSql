package com.demo.csvtosql.utils;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

public class ErrorDataUtil {
    Logger logger = LoggerFactory.getLogger(getClass());

    private static ErrorDataUtil util;

    private static Queue<String[]> errorLines = new ArrayBlockingQueue<>(1050);

    private static String errDataPath;

    private static String[] headers;

    public static void init(String errDataPath, String[] headers) {
        ErrorDataUtil.errDataPath = errDataPath;
        util = new ErrorDataUtil();
        ErrorDataUtil.headers = headers;
    }

    public static ErrorDataUtil getInstance() {
        if (util == null) {
            throw new RuntimeException("util not init");
        }
        return util;
    }

    //错误数据入缓存
    public void addErrData(String[] line) {
        if (!errorLines.offer(line)) {
            //队列满了还没处理,打印错误日志
            //也可以放入缓存
            logger.error("磁盘无法写入,错误队列已满,请检查;当前错误数据:{}", line);
            return;
        }
        //大于一定条数写入到错误记录
        if (errorLines.size() >= 1000) {
            flushErrorData();
        }
    }

    //将错误数据写入文件
    public void flushErrorData() {
        if (errorLines.isEmpty()) {
            return;
        }
        FileWriter writer = null;
        boolean isNew=false;
        try {
            File file = new File(errDataPath);
            if (!file.exists()) {
                createErrFile();
                isNew=true;
            }
            writer = new FileWriter(file);
            if(isNew){
                writer.write(String.join(",", headers)+"\r\n");
            }
            while (!errorLines.isEmpty()) {
                String[] poll = errorLines.poll();
                if (poll != null) {
                    writer.write(String.join(",", poll)+"\r\n");
                }
            }
            writer.flush();
            errorLines.clear();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void createErrFile() {
        File errFile = new File(errDataPath);
        if (!errFile.exists()) {
            try {
                File parentFile = errFile.getParentFile();
                if (!parentFile.exists()) {
                    parentFile.mkdirs();
                }
                errFile.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException("create error file failed");
            }
        }
    }

    private ErrorDataUtil() {

    }
}
