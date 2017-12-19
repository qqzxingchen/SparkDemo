package com.gp.sparkdemo.core;

import com.gp.sparkdemo.util.JavaSparkConTextGenerator;
import com.gp.sparkdemo.util.TestDataGenerateTool;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * coalesce 算子，将 RDD 的分区重新划分
 */
public class coalesce {
    public static void main(String[] args){

        JavaSparkContext sc = JavaSparkConTextGenerator.generateLocalSparkContext();

        // 默认生成的 RDD 为 10 个分区
        JavaRDD<Integer> input = sc.parallelize(TestDataGenerateTool.range(10,100000),10);
        System.out.println( String.format("input partition current num %s",input.getNumPartitions()) );


// 为什么这里设置的不生效 ???
        input.coalesce( 20,Boolean.TRUE );
        System.out.println( String.format("input partition current num %s",input.getNumPartitions()) );

//        input.coalesce( 20 );
//        System.out.println( String.format("input partition current num %s",input.getNumPartitions()) );


        input.repartition( 20 );
        System.out.println( String.format("input partition current num %s",input.getNumPartitions()) );


        input.collect();
        System.out.println( String.format("input partition current num %s",input.getNumPartitions()) );
    }
}
