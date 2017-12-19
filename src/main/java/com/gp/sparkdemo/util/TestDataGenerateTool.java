package com.gp.sparkdemo.util;

import java.util.ArrayList;
import java.util.List;

/**
 * 通过一些简便的方式生成一些测试用的数组
 */
public class TestDataGenerateTool {

    public static List<Integer> range(int start,int end){
        List<Integer> rangeList = new ArrayList<Integer>();
        for ( Integer index = start ; index < end ; index ++){
            rangeList.add( index );
        }
        return rangeList;
    }


    public static void main(String[] args){
        System.out.println( range(10,20).toString() );
    }

}
