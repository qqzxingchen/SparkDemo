package com.gp.sparkdemo.core;

import com.gp.sparkdemo.util.JavaSparkConTextGenerator;
import com.gp.sparkdemo.util.TestDataGenerateTool;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

/**
 * filter 算子，根据条件筛选rdd，返回源 rdd 的一个子集
 */
public class filter {
    public static void main(String[] args){
        JavaSparkContext sc = JavaSparkConTextGenerator.generateLocalSparkContext();

        JavaRDD<Integer> input = sc.parallelize(TestDataGenerateTool.range(10,100));

        JavaRDD<Integer> filteredInput = input.filter(new Function<Integer, Boolean>() {
            @Override
            public Boolean call(Integer v1) throws Exception {
                return v1 % 3 == 0;
            }
        });

        System.out.println( filteredInput.collect().toString() );

    }
}
