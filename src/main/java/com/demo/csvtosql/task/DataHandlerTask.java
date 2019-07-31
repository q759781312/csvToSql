package com.demo.csvtosql.task;

import com.demo.csvtosql.utils.ErrorDataUtil;

import java.util.Queue;

public class DataHandlerTask implements Runnable {

    private Queue<String[]> lines;
    private String[] headers;
    private String tableName;

    public DataHandlerTask(Queue<String[]> lines, String[] headers, String tableName) {
        this.lines = lines;
        this.headers = headers;
        this.tableName = tableName;
    }

    public void run() {
        while (!lines.isEmpty()) {
            StringBuilder sqlStr = new StringBuilder();
            String[] contents = lines.poll();
            try {
                if(headers.length!=contents.length){
                    throw new RuntimeException("标题与正文个数不符!");
                }
                sqlStr.append(" insert into ").append(tableName).append(" (");
                for (String header : headers) {
                    sqlStr.append("`").append(header).append("`").append(",");
                }
                sqlStr = sqlStr.replace(sqlStr.length() - 1, sqlStr.length(), "");
                sqlStr.append(") values (");
                for (String content : contents) {
                    sqlStr.append("\"").append(content).append("\"").append(",");
                }
                sqlStr = sqlStr.replace(sqlStr.length() - 1, sqlStr.length(), "");
                sqlStr.append(");");
                System.out.println(sqlStr.toString());
            } catch (Exception e) {
                ErrorDataUtil.getInstance().addErrData(contents);
            }
        }
    }
}
