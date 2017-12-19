package com.gp.sparkdemo.core;

import com.gp.sparkdemo.util.JavaSparkConTextGenerator;
import com.gp.sparkdemo.util.TestDataGenerateTool;
import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * mapPartitions 算子
 * 该函数和 map 函数类似，只不过映射函数的参数由RDD中的每一个元素变成了RDD中每一个分区的迭代器。
 * 在实际中，如果需要将 rdd 中的每个数据都写入到数据库中，那么使用 map 就不合适，因为需要为每次 map 都创建一个 jdbc 链接
 *      但是使用 mapPartitions 就可以在一个分区内建立一个 jdbc 链接
 */
public class mapPartitions {
    public static void main(String[] args){
        JavaSparkContext sc = JavaSparkConTextGenerator.generateLocalSparkContext();

        // 指定为 10 个分区
        JavaRDD<Integer> input = sc.parallelize(TestDataGenerateTool.range(10,1000),10);

        JavaRDD<String> result = input.mapPartitions(new FlatMapFunction<Iterator<Integer>, String>() {
            @Override
            public Iterator<String> call(Iterator<Integer> curPartitionData){
                List<Integer> curPartitionDataList = IteratorUtils.toList(curPartitionData);
                String outputStr = String.format( "CurPartitionData: %s",curPartitionDataList.toString() );
                return new ArrayList<String>(){{
                    add( outputStr );
                }}.iterator();
            }
        });

        for ( String str : result.collect() ){
            System.out.println( str.toString() );
        }
    }
}
