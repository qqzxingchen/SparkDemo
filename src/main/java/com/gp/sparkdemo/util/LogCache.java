package com.gp.sparkdemo.util;

import java.util.ArrayList;
import java.util.List;

/**
 * 日志缓存
 */
public class LogCache {
    private static List<String> logs = new ArrayList<>();

    public static void clearLogs(){
        logs.clear();
    }

    public static void addLog(String log) {
        logs.add( log );
    }

    public static List<String> getLogs() {
        return logs;
    }

    public static void printLogs(){
        for ( String log : getLogs() ){
            System.out.println( String.format( "LogCache: %s",log ) );
        }
    }
}
