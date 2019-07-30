package com.demo.csvtosql.task;

import com.demo.csvtosql.utils.ErrorDataUtil;

import java.util.Queue;

public class DataHandlerTask implements Runnable {

    private Queue<String[]> lines;
    private String[] headers;

    public DataHandlerTask(Queue<String[]> lines, String[] headers) {
        this.lines = lines;
        this.headers = headers;
    }

    public void run() {
        while (!lines.isEmpty()) {
            StringBuilder sqlStr = new StringBuilder();
            String[] contents = lines.poll();
            try {
                sqlStr.append(" insert into demo (");
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
                int i=1/0;
                System.out.println(sqlStr.toString());
            } catch (Exception e) {
                ErrorDataUtil.getInstance().addErrData(contents);
            }
        }
    }
}
