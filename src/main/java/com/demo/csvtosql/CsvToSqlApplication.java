package com.demo.csvtosql;

import com.demo.csvtosql.task.DataHandlerTask;
import com.demo.csvtosql.utils.ErrorDataUtil;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CsvToSqlApplication {
    private static String csvPath = "C:\\Users\\myemi\\Desktop\\demo.csv";
    private static String errDataPath = "C:\\Users\\myemi\\Desktop\\error.csv";
    private static int maxColumns = 1000;

    public static void main(String[] args) {
        BufferedReader reader = null;
        //初始化工具类
        try {
            //读取文件流
            reader = new BufferedReader(new FileReader(csvPath));
            String[] headers = reader.readLine().split(",");//第一行信息，为标题信息
            ErrorDataUtil.init(errDataPath, headers);
            String lineStr = null;
            Queue<String[]> lines = new ArrayBlockingQueue<String[]>(maxColumns);
            //线程池管理多线程
            ExecutorService threadPool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
            while ((lineStr = reader.readLine()) != null) {
                //获取数据
                String[] line = lineStr.split(",");
                //队列添加失败,放入错误处理
                if (!lines.offer(line)) {
                    ErrorDataUtil.getInstance().addErrData(line);
                    continue;
                }
                //当队列满了,开启线程处理
                if (lines.size() >= maxColumns) {
                    threadPool.submit(new DataHandlerTask(lines, headers));
                    lines = new ArrayBlockingQueue<String[]>(maxColumns);
                }
            }
            if (!lines.isEmpty()) {
                threadPool.execute(new DataHandlerTask(lines, headers));
            }

            threadPool.shutdown();
            while (!threadPool.isTerminated()) {
                Thread.sleep(500);
            }
            ErrorDataUtil.getInstance().flushErrorData();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
