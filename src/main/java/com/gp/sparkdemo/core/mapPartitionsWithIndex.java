package com.gp.sparkdemo.core;

import com.gp.sparkdemo.util.JavaSparkConTextGenerator;
import com.gp.sparkdemo.util.TestDataGenerateTool;
import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * mapPartitionsWithIndex 算子
 * 基本与 mapPartitions 相同，只是每个分区都带了一个序号
 */
public class mapPartitionsWithIndex {
    public static void main(String[] args){
        JavaSparkContext sc = JavaSparkConTextGenerator.generateLocalSparkContext();

        // 指定为 10 个分区
        JavaRDD<Integer> input = sc.parallelize(TestDataGenerateTool.range(10,1000),10);


        JavaRDD<String> result = input.mapPartitionsWithIndex(new Function2<Integer, Iterator<Integer>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer index, Iterator<Integer> curPartitionData) throws Exception {
                List<Integer> curPartitionDataList = IteratorUtils.toList(curPartitionData);
                String outputStr = String.format( "CurPartitionData[%s]: %s",index,curPartitionDataList.toString() );
                return new ArrayList<String>(){{
                    add( outputStr );
                }}.iterator();
            }
        },false);

        for ( String str : result.collect() ){
            System.out.println( str );
        }
    }
}
