package com.gp.sparkdemo.core;

import com.gp.sparkdemo.util.JavaSparkConTextGenerator;
import com.gp.sparkdemo.util.TestDataGenerateTool;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * union 算子
 * 将两个 rdd 做并集，注意，不会去重
 */
public class union {
    public static void main(String[] args){
        JavaSparkContext sc = JavaSparkConTextGenerator.generateLocalSparkContext();

        JavaRDD<Integer> input1 = sc.parallelize(TestDataGenerateTool.range(1,11));
        JavaRDD<Integer> input2 = sc.parallelize(TestDataGenerateTool.range(5,15));

        System.out.println( input1.union( input2 ).collect().toString() );
    }
}
